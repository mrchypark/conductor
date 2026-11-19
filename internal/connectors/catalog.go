// Package connectors adapts the Ampersand open connector catalog to Conductor.
package connectors

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sort"
	"sync"
	"time"

	amp "github.com/amp-labs/connectors"
	"github.com/amp-labs/connectors/common"
	"github.com/amp-labs/connectors/common/substitutions/catalogreplacer"
	"github.com/amp-labs/connectors/connector"
	"github.com/amp-labs/connectors/generic"
	"github.com/amp-labs/connectors/providers"
	gowid "github.com/mrchypark/gowid-go-sdk/client"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

type Catalog struct {
	mu       sync.RWMutex
	infos    map[string]providers.ProviderInfo
	custom   map[string]map[string]providers.ProviderInfo
	business *BusinessClient
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
	return &Catalog{
		infos:  infos,
		custom: make(map[string]map[string]providers.ProviderInfo),
	}, nil
}

func (c *Catalog) SetBusinessClient(client *BusinessClient) { c.business = client }

func (c *Catalog) Names() []string {
	return c.NamesForWorkspace("")
}

func (c *Catalog) NamesForWorkspace(workspaceID string) []string {
	c.mu.RLock()
	seen := make(map[string]bool, len(c.infos))
	for name := range c.infos {
		seen[name] = true
	}
	if workspaceID != "" {
		for name := range c.custom[workspaceID] {
			seen[name] = true
		}
	}
	c.mu.RUnlock()
	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (c *Catalog) Info(name string, vars ...catalogreplacer.CatalogVariable) (*providers.ProviderInfo, error) {
	return c.InfoForWorkspace("", name, vars...)
}

func (c *Catalog) RegisterCustom(workspaceID, name string, info providers.ProviderInfo, allowInsecureHTTP bool) error {
	if workspaceID == "" || name == "" {
		return errors.New("workspace_id and provider name are required")
	}
	u, err := url.Parse(info.BaseURL)
	if err != nil || u.Host == "" || u.User != nil || u.Fragment != "" {
		return errors.New("custom connector base_url must be a valid URL")
	}
	if u.Scheme != "https" && (!allowInsecureHTTP || u.Scheme != "http") {
		return errors.New("custom connector base_url must use https")
	}
	if info.AuthType == "" {
		info.AuthType = providers.None
	}
	info.Name = name
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.custom == nil {
		c.custom = make(map[string]map[string]providers.ProviderInfo)
	}
	wsMap := c.custom[workspaceID]
	if wsMap == nil {
		wsMap = make(map[string]providers.ProviderInfo)
		c.custom[workspaceID] = wsMap
	}
	wsMap[name] = info
	return nil
}

func (c *Catalog) InfoForWorkspace(workspaceID, name string, vars ...catalogreplacer.CatalogVariable) (*providers.ProviderInfo, error) {
	if workspaceID != "" {
		c.mu.RLock()
		if wsMap := c.custom[workspaceID]; wsMap != nil {
			if info, ok := wsMap[name]; ok {
				c.mu.RUnlock()
				copyInfo := info
				return &copyInfo, nil
			}
		}
		c.mu.RUnlock()
	}
	c.mu.RLock()
	_, ok := c.infos[name]
	c.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: %s", providers.ErrProviderNotFound, name)
	}
	return providers.ReadInfo(name, vars...)
}

type Credentials struct {
	Validate     func(context.Context) error
	TokenSource  common.TokenSourceWithContext
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
	return c.NewWithModuleForWorkspace(ctx, "", name, "", client, workspace, metadata, creds)
}

// NewWithModule selects a provider module when a connector exposes more than
// one API surface (for example Google's calendar and Gmail modules).
func (c *Catalog) NewWithModule(ctx context.Context, name, module string, client *http.Client, workspace string, metadata map[string]string, creds Credentials) (amp.Connector, error) {
	return c.NewWithModuleForWorkspace(ctx, "", name, module, client, workspace, metadata, creds)
}

func (c *Catalog) NewWithModuleForWorkspace(ctx context.Context, scopeWorkspace, name, module string, client *http.Client, workspace string, metadata map[string]string, creds Credentials) (amp.Connector, error) {
	info, err := c.InfoForWorkspace(scopeWorkspace, name)
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
	c.mu.RLock()
	_, isBuiltin := c.infos[name]
	c.mu.RUnlock()
	if !isBuiltin {
		return &generic.Connector{
			ProviderInfo: info,
			Client: &common.JSONHTTPClient{
				HTTPClient: &common.HTTPClient{
					Base:   info.BaseURL,
					Client: authClient,
				},
			},
		}, nil
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
	if creds.Validate != nil {
		copy := *client
		base := copy.Transport
		if base == nil {
			base = http.DefaultTransport
		}
		copy.Transport = profileGuardTransport{base: base, validate: creds.Validate}
		client = &copy
	}
	if info.AuthType == providers.Oauth2 && creds.TokenSource != nil {
		return common.NewOAuthHTTPClient(ctx, common.WithOAuthClient(client), common.WithTokenSource(creds.TokenSource))
	}
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

type profileGuardTransport struct {
	base     http.RoundTripper
	validate func(context.Context) error
}

func (t profileGuardTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if err := t.validate(r.Context()); err != nil {
		return nil, err
	}
	return t.base.RoundTrip(r)
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

// NewSSRFSafeHTTPClient returns an HTTP client that disables redirects and
// optionally blocks loopback, private, and link-local IP addresses on dial.
func NewSSRFSafeHTTPClient(base *http.Client, allowPrivate bool) *http.Client {
	var copyClient http.Client
	if base != nil {
		copyClient = *base
	}
	if copyClient.Timeout == 0 {
		copyClient.Timeout = 30 * time.Second
	}
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	if !allowPrivate && copyClient.Transport == nil {
		dialer := &net.Dialer{Timeout: 10 * time.Second}
		copyClient.Transport = &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				host, port, err := net.SplitHostPort(addr)
				if err != nil {
					return nil, err
				}
				ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
				if err != nil {
					return nil, err
				}
				for _, ip := range ips {
					if isDisallowedIP(ip.IP) {
						return nil, fmt.Errorf("blocked private or link-local address %s", ip.IP.String())
					}
				}
				for _, ip := range ips {
					conn, dialErr := dialer.DialContext(ctx, network, net.JoinHostPort(ip.IP.String(), port))
					if dialErr == nil {
						return conn, nil
					}
					err = dialErr
				}
				return nil, err
			},
		}
	}
	return &copyClient
}

func isDisallowedIP(ip net.IP) bool {
	if ip == nil {
		return true
	}
	return ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() || ip.IsMulticast()
}
