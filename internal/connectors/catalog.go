// Package connectors adapts the Ampersand open connector catalog to Conductor.
package connectors

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"

	amp "github.com/amp-labs/connectors"
	"github.com/amp-labs/connectors/common"
	"github.com/amp-labs/connectors/common/substitutions/catalogreplacer"
	"github.com/amp-labs/connectors/connector"
	"github.com/amp-labs/connectors/generic"
	"github.com/amp-labs/connectors/providers"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
	gowid "gowid-api-go/client"
)

type Catalog struct {
	infos  map[string]providers.ProviderInfo
	compos *ComposClient
}

func init() {
	providers.SetInfo("gowid", providers.ProviderInfo{
		DisplayName: "Gowid", AuthType: providers.ApiKey, BaseURL: gowid.BaseURL,
		ApiKeyOpts: &providers.ApiKeyOpts{AttachmentType: providers.Header, Header: &providers.ApiKeyOptsHeader{Name: "Authorization"}},
		Support:    providers.Support{Proxy: true},
	})
}

func LoadCatalog() (*Catalog, error) {
	names := providers.AllNames()
	infos := make(map[string]providers.ProviderInfo, len(names))
	for _, name := range names {
		info, err := providers.ReadInfo(name)
		if err != nil {
			return nil, fmt.Errorf("read provider %q: %w", name, err)
		}
		infos[name] = *info
	}
	return &Catalog{infos: infos}, nil
}

func (c *Catalog) SetComposClient(client *ComposClient) { c.compos = client }

func (c *Catalog) Names() []string {
	names := make([]string, 0, len(c.infos))
	for name := range c.infos {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (c *Catalog) Info(name string, vars ...catalogreplacer.CatalogVariable) (*providers.ProviderInfo, error) {
	if _, ok := c.infos[name]; !ok {
		return nil, fmt.Errorf("%w: %s", providers.ErrProviderNotFound, name)
	}
	return providers.ReadInfo(name, vars...)
}

type Credentials struct {
	OAuthConfig  *oauth2.Config
	OAuthToken   *oauth2.Token
	TokenUpdated func(oldToken, newToken *oauth2.Token) error
	APIKey       string
	BasicUser    string
	BasicPass    string
	Custom       map[string]string
	Metadata     map[string]string
}

type Manifest struct {
	Name        string             `json:"name"`
	DisplayName string             `json:"display_name,omitempty"`
	BaseURL     string             `json:"base_url"`
	AuthType    providers.AuthType `json:"auth_type"`
	Support     providers.Support  `json:"support"`
}

func (c *Catalog) Manifest(name string) (Manifest, error) {
	info, err := c.Info(name)
	if err != nil {
		return Manifest{}, err
	}
	return Manifest{Name: info.Name, DisplayName: info.DisplayName, BaseURL: info.BaseURL, AuthType: info.AuthType, Support: info.Support}, nil
}

// New builds the static Amp factory when an implementation exists. Providers
// that have catalog metadata but no deep adapter use Amp's generic raw HTTP
// connector, so catalog coverage is not silently lost.
func (c *Catalog) New(ctx context.Context, name string, client *http.Client, workspace string, metadata map[string]string, creds Credentials) (amp.Connector, error) {
	return c.NewWithModule(ctx, name, "", client, workspace, metadata, creds)
}

// NewWithModule selects a provider module when a connector exposes more than
// one API surface (for example Google's calendar and Gmail modules).
func (c *Catalog) NewWithModule(ctx context.Context, name, module string, client *http.Client, workspace string, metadata map[string]string, creds Credentials) (amp.Connector, error) {
	info, err := c.Info(name)
	if err != nil {
		return nil, err
	}
	if client == nil {
		client = http.DefaultClient
	}
	authClient, err := newAuthClient(ctx, info, client, creds)
	if err != nil {
		return nil, err
	}
	params := common.ConnectorParams{
		AuthenticatedClient: authClient,
		Workspace:           workspace,
		Module:              common.ModuleID(module),
		Metadata:            metadata,
	}
	conn, err := connector.New(name, params)
	if err == nil {
		return conn, nil
	}
	if !errors.Is(err, connector.ErrInvalidProvider) {
		return nil, err
	}

	opts := []generic.Option{generic.WithAuthenticatedClient(authClient)}
	if workspace != "" {
		opts = append(opts, generic.WithWorkspace(workspace))
	}
	if len(metadata) > 0 {
		opts = append(opts, generic.WithMetadata(metadata))
	}
	return generic.NewConnector(name, opts...)
}

func newAuthClient(ctx context.Context, info *providers.ProviderInfo, client *http.Client, creds Credentials) (common.AuthenticatedHTTPClient, error) {
	params := &providers.NewClientParams{Client: client}
	switch info.AuthType {
	case providers.Oauth2:
		if info.Oauth2Opts == nil {
			return nil, errors.New("provider has no OAuth2 options")
		}
		switch info.Oauth2Opts.GrantType {
		case providers.ClientCredentials:
			if creds.OAuthConfig == nil {
				return nil, errors.New("OAuth client credentials require OAuth config")
			}
			config := clientcredentials.Config{
				ClientID:     creds.OAuthConfig.ClientID,
				ClientSecret: creds.OAuthConfig.ClientSecret,
				TokenURL:     info.Oauth2Opts.TokenURL,
				Scopes:       creds.OAuthConfig.Scopes,
			}
			if config.TokenURL == "" {
				config.TokenURL = creds.OAuthConfig.Endpoint.TokenURL
			}
			params.OAuth2ClientCreds = &providers.OAuth2ClientCredentialsParams{Config: &config}
			if creds.TokenUpdated != nil {
				params.OAuth2ClientCreds.Options = append(params.OAuth2ClientCreds.Options, common.WithTokenUpdated(creds.TokenUpdated))
			}
		default:
			if creds.OAuthConfig == nil || creds.OAuthToken == nil {
				return nil, errors.New("OAuth authorization-code providers require an OAuth config and token")
			}
			params.OAuth2AuthCodeCreds = &providers.OAuth2AuthCodeParams{Config: creds.OAuthConfig, Token: creds.OAuthToken}
			if creds.TokenUpdated != nil {
				params.OAuth2AuthCodeCreds.Options = append(params.OAuth2AuthCodeCreds.Options, common.WithTokenUpdated(creds.TokenUpdated))
			}
		}
	case providers.ApiKey:
		params.ApiKeyCreds = &providers.ApiKeyParams{Key: creds.APIKey}
	case providers.Basic:
		params.BasicCreds = &providers.BasicParams{User: creds.BasicUser, Pass: creds.BasicPass}
	case providers.Custom:
		params.CustomCreds = &providers.CustomAuthParams{Values: creds.Custom}
	}
	return info.NewClient(ctx, params)
}

type Capabilities struct {
	Read      bool `json:"read"`
	Write     bool `json:"write"`
	Delete    bool `json:"delete"`
	Subscribe bool `json:"subscribe"`
	Proxy     bool `json:"proxy"`
}

func CapabilitiesOf(conn amp.Connector) Capabilities {
	_, read := conn.(amp.ReadConnector)
	_, write := conn.(amp.WriteConnector)
	_, del := conn.(amp.DeleteConnector)
	_, subscribe := conn.(amp.SubscribeConnector)
	_, proxy := conn.(amp.ProxyConnector)
	return Capabilities{Read: read, Write: write, Delete: del, Subscribe: subscribe, Proxy: proxy}
}
