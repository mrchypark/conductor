package connectors

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"slices"
	"strings"
	"syscall"

	"github.com/amp-labs/connectors/generic"
	"github.com/amp-labs/connectors/providers"
	"github.com/mrchypark/conductor/internal/handlers"
)

// CredentialProviderConfig is trusted operator configuration. It is not execution input.
type CredentialProviderConfig struct {
	Issuer   string                      `json:"issuer"`
	Bindings []CredentialProviderBinding `json:"bindings"`
}

// CredentialProviderBinding pins one connection revision to one reviewed operation.
type CredentialProviderBinding struct {
	ConnectionID string                `json:"connection_id"`
	WorkspaceID  string                `json:"workspace_id"`
	Revision     int64                 `json:"revision"`
	OperationKey string                `json:"operation_key"`
	TokenFile    string                `json:"token_file"`
	APIKey       *DeliveryBinding      `json:"api_key,omitempty"`
	OAuth        *OAuthDeliveryBinding `json:"oauth,omitempty"`
}

type credentialProviderDelivery struct {
	binding CredentialProviderBinding
	invoke  func(context.Context) ([]byte, error)
}

// NewCredentialProviderHandler installs only operator-reviewed delivery bindings. It
// reads a current human authority token for every invocation and never accepts
// a destination or credential from execution input.
func NewCredentialProviderHandler(config CredentialProviderConfig, authorityClient, providerClient *http.Client) (handlers.Handler, error) {
	return newCredentialProviderHandler(config, authorityClient, providerClient, false, false, nil)
}

type humanTokenKey struct{}

// WithHumanToken attaches the human authority token to a single request context.
func WithHumanToken(ctx context.Context, token string) context.Context {
	return context.WithValue(ctx, humanTokenKey{}, token)
}

// NewCredentialProviderHandlerV2 obtains human authority tokens only from request contexts.
func NewCredentialProviderHandlerV2(config CredentialProviderConfig, authorityClient, providerClient *http.Client) (handlers.Handler, error) {
	return newCredentialProviderHandler(config, authorityClient, providerClient, true, false, nil)
}

// NewCredentialProviderHandlerV3 routes reviewed OAuth requests through AMP.
// Unknown providers fail per invocation, allowing other bindings and @2 to work.
func NewCredentialProviderHandlerV3(config CredentialProviderConfig, authorityClient, providerClient *http.Client) (handlers.Handler, error) {
	return newCredentialProviderHandler(config, authorityClient, providerClient, true, true, nil)
}

// NewCredentialProviderHandlers shares OAuth version guards between versions 2 and 3.
func NewCredentialProviderHandlers(config CredentialProviderConfig, authorityClient, providerClient *http.Client) (handlers.Handler, handlers.Handler, error) {
	clients := make(map[string]*http.Client)
	v2, err := newCredentialProviderHandler(config, authorityClient, providerClient, true, false, clients)
	if err != nil {
		return nil, nil, err
	}
	v3, err := newCredentialProviderHandler(config, authorityClient, providerClient, true, true, clients)
	if err != nil {
		return nil, nil, err
	}
	return v2, v3, nil
}

func newCredentialProviderHandler(config CredentialProviderConfig, authorityClient, providerClient *http.Client, contextToken, ampOAuth bool, clients map[string]*http.Client) (handlers.Handler, error) {
	if config.Issuer == "" || len(config.Bindings) == 0 {
		return nil, errors.New("credential provider configuration requires issuer and bindings")
	}
	deliveries := make(map[string]credentialProviderDelivery, len(config.Bindings))
	for _, binding := range config.Bindings {
		if binding.ConnectionID == "" || binding.WorkspaceID == "" || binding.Revision < 1 || binding.OperationKey == "" || (!contextToken && binding.TokenFile == "") || (binding.APIKey == nil) == (binding.OAuth == nil) {
			return nil, errors.New("credential provider binding invalid")
		}
		if _, exists := deliveries[binding.ConnectionID]; exists {
			return nil, errors.New("credential provider connection binding duplicated")
		}
		source := func(ctx context.Context) (string, error) { return readCredentialProviderToken(ctx, binding.TokenFile) }
		if contextToken {
			source = func(ctx context.Context) (string, error) {
				token, _ := ctx.Value(humanTokenKey{}).(string)
				if !oauthBearer(token) {
					return "", errors.New("token unavailable")
				}
				return token, nil
			}
		}
		delivery := credentialProviderDelivery{binding: binding}
		if binding.APIKey != nil {
			client, err := NewAPIKeyDelivery(config.Issuer, *binding.APIKey, source, authorityClient, providerClient)
			if err != nil {
				return nil, errors.New("credential provider API-key binding invalid")
			}
			delivery.invoke = func(ctx context.Context) ([]byte, error) { return client.Invoke(ctx) }
		} else {
			client := clients[binding.ConnectionID]
			if client == nil {
				var err error
				client, err = NewOAuthDeliveryClient(config.Issuer, *binding.OAuth, func(ctx context.Context, _ OAuthTokenRequest) (string, error) { return source(ctx) }, authorityClient, providerClient, nil)
				if err != nil {
					return nil, errors.New("credential provider OAuth binding invalid")
				}
				if clients != nil {
					clients[binding.ConnectionID] = client
				}
			}
			endpoint := binding.OAuth.Endpoint
			if ampOAuth {
				if !slices.Contains(providers.AllNames(), binding.OAuth.ProviderID) {
					delivery.invoke = func(context.Context) ([]byte, error) {
						return nil, errors.New("credential provider AMP provider unsupported")
					}
					deliveries[binding.ConnectionID] = delivery
					continue
				}
				conn, err := generic.NewConnector(binding.OAuth.ProviderID, generic.WithAuthenticatedClient(client))
				if err != nil {
					return nil, errors.New("credential provider AMP binding invalid")
				}
				delivery.invoke = func(ctx context.Context) ([]byte, error) {
					_, body, err := conn.HTTPClient().Get(ctx, endpoint)
					return body, err
				}
				deliveries[binding.ConnectionID] = delivery
				continue
			}
			delivery.invoke = func(ctx context.Context) ([]byte, error) {
				req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
				if err != nil {
					return nil, errors.New("credential provider SaaS request invalid")
				}
				response, err := client.Do(req)
				if err != nil {
					return nil, err
				}
				defer response.Body.Close()
				return readDeliveryBody(response.Body, 1<<20)
			}
		}
		deliveries[binding.ConnectionID] = delivery
	}
	version := "1"
	if contextToken {
		version = "2"
	}
	if ampOAuth {
		version = "3"
	}
	return handlers.NewFuncHandler(handlers.Manifest{Key: "connector.credentials.invoke", Version: version, DefaultTimeout: 30, Idempotency: "provider-defined", ResourceClass: "network"}, func(ctx context.Context, request handlers.Request) (handlers.Result, error) {
		if input := strings.TrimSpace(string(request.Input)); input != "" {
			var object map[string]json.RawMessage
			if json.Unmarshal([]byte(input), &object) != nil || object == nil || len(object) != 0 {
				return handlers.Result{}, &handlers.Error{Class: "invalid_input", Message: "credential provider delivery input must be empty"}
			}
		}
		delivery, ok := deliveries[request.ConnectionID]
		if !ok || request.WorkspaceID != delivery.binding.WorkspaceID || request.CredentialRevision != delivery.binding.Revision || request.OperationKey != delivery.binding.OperationKey {
			return handlers.Result{}, &handlers.Error{Class: "connection_error", Message: "credential provider binding does not match execution"}
		}
		output, err := delivery.invoke(ctx)
		if err != nil {
			return handlers.Result{}, &handlers.Error{Class: "connection_error", Message: "credential provider credential delivery failed"}
		}
		return handlers.Result{Output: output}, nil
	})
}

func readCredentialProviderToken(ctx context.Context, path string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return "", errors.New("token unavailable")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return "", errors.New("token unavailable")
	}
	body, err := io.ReadAll(io.LimitReader(file, 8193))
	if err != nil || len(body) > 8192 {
		return "", errors.New("token unavailable")
	}
	token := strings.TrimRight(string(body), "\r\n")
	if !oauthBearer(token) {
		return "", errors.New("token unavailable")
	}
	return token, nil
}
