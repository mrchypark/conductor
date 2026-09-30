package connectors

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
	"syscall"

	"github.com/mrchypark/conductor/internal/handlers"
)

// GoAuthyConfig is trusted operator configuration. It is not execution input.
type GoAuthyConfig struct {
	Issuer   string           `json:"issuer"`
	Bindings []GoAuthyBinding `json:"bindings"`
}

// GoAuthyBinding pins one connection revision to one reviewed operation.
type GoAuthyBinding struct {
	ConnectionID string                `json:"connection_id"`
	WorkspaceID  string                `json:"workspace_id"`
	Revision     int64                 `json:"revision"`
	OperationKey string                `json:"operation_key"`
	TokenFile    string                `json:"token_file"`
	APIKey       *DeliveryBinding      `json:"api_key,omitempty"`
	OAuth        *OAuthDeliveryBinding `json:"oauth,omitempty"`
}

type goauthyDelivery struct {
	binding GoAuthyBinding
	invoke  func(context.Context) ([]byte, error)
}

// NewGoAuthyHandler installs only operator-reviewed delivery bindings. It
// reads a current human authority token for every invocation and never accepts
// a destination or credential from execution input.
func NewGoAuthyHandler(config GoAuthyConfig, authorityClient, providerClient *http.Client) (handlers.Handler, error) {
	if config.Issuer == "" || len(config.Bindings) == 0 {
		return nil, errors.New("GoAuthy configuration requires issuer and bindings")
	}
	deliveries := make(map[string]goauthyDelivery, len(config.Bindings))
	for _, binding := range config.Bindings {
		if binding.ConnectionID == "" || binding.WorkspaceID == "" || binding.Revision < 1 || binding.OperationKey == "" || binding.TokenFile == "" || (binding.APIKey == nil) == (binding.OAuth == nil) {
			return nil, errors.New("GoAuthy binding invalid")
		}
		if _, exists := deliveries[binding.ConnectionID]; exists {
			return nil, errors.New("GoAuthy connection binding duplicated")
		}
		source := func(ctx context.Context) (string, error) { return readGoAuthyToken(ctx, binding.TokenFile) }
		delivery := goauthyDelivery{binding: binding}
		if binding.APIKey != nil {
			client, err := NewAPIKeyDelivery(config.Issuer, *binding.APIKey, source, authorityClient, providerClient)
			if err != nil {
				return nil, errors.New("GoAuthy API-key binding invalid")
			}
			delivery.invoke = func(ctx context.Context) ([]byte, error) { return client.Invoke(ctx) }
		} else {
			client, err := NewOAuthDeliveryClient(config.Issuer, *binding.OAuth, func(ctx context.Context, _ OAuthTokenRequest) (string, error) { return source(ctx) }, authorityClient, providerClient, nil)
			if err != nil {
				return nil, errors.New("GoAuthy OAuth binding invalid")
			}
			endpoint := binding.OAuth.Endpoint
			delivery.invoke = func(ctx context.Context) ([]byte, error) {
				req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
				if err != nil {
					return nil, errors.New("GoAuthy provider request invalid")
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
	return handlers.NewFuncHandler(handlers.Manifest{Key: "connector.goauthy.invoke", Version: "1", DefaultTimeout: 30, Idempotency: "provider-defined", ResourceClass: "network"}, func(ctx context.Context, request handlers.Request) (handlers.Result, error) {
		if input := strings.TrimSpace(string(request.Input)); input != "" {
			var object map[string]json.RawMessage
			if json.Unmarshal([]byte(input), &object) != nil || object == nil || len(object) != 0 {
				return handlers.Result{}, &handlers.Error{Class: "invalid_input", Message: "GoAuthy delivery input must be empty"}
			}
		}
		delivery, ok := deliveries[request.ConnectionID]
		if !ok || request.WorkspaceID != delivery.binding.WorkspaceID || request.CredentialRevision != delivery.binding.Revision || request.OperationKey != delivery.binding.OperationKey {
			return handlers.Result{}, &handlers.Error{Class: "connection_error", Message: "GoAuthy binding does not match execution"}
		}
		output, err := delivery.invoke(ctx)
		if err != nil {
			return handlers.Result{}, &handlers.Error{Class: "connection_error", Message: "GoAuthy credential delivery failed"}
		}
		return handlers.Result{Output: output}, nil
	})
}

func readGoAuthyToken(ctx context.Context, path string) (string, error) {
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
