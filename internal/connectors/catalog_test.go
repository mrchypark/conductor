package connectors

import (
	"context"
	"net/http"
	"testing"

	"github.com/amp-labs/connectors/providers"
	"golang.org/x/oauth2"
)

func TestLoadCatalogIncludesAmpProviders(t *testing.T) {
	catalog, err := LoadCatalog()
	if err != nil {
		t.Fatal(err)
	}
	names := catalog.Names()
	if len(names) != len(providers.AllNames()) {
		t.Fatalf("catalog dropped Amp providers: got %d want %d", len(names), len(providers.AllNames()))
	}
	if names[0] == "" || names[len(names)-1] == "" {
		t.Fatal("catalog contains an empty provider name")
	}
}

func TestNewAuthClientSupportsClientCredentialsGrant(t *testing.T) {
	info := &providers.ProviderInfo{AuthType: providers.Oauth2, Oauth2Opts: &providers.Oauth2Opts{GrantType: providers.ClientCredentials, TokenURL: "https://token.example.test"}}
	_, err := newAuthClient(context.Background(), info, http.DefaultClient, Credentials{OAuthConfig: &oauth2.Config{ClientID: "client", ClientSecret: "secret"}})
	if err != nil {
		t.Fatal(err)
	}
}

func TestNewWithModuleSelectsMultiModuleProvider(t *testing.T) {
	catalog, err := LoadCatalog()
	if err != nil {
		t.Fatal(err)
	}
	conn, err := catalog.NewWithModule(context.Background(), "google", "gmail", nil, "", nil, Credentials{
		OAuthConfig: &oauth2.Config{ClientID: "client", ClientSecret: "secret"}, OAuthToken: &oauth2.Token{AccessToken: "token"},
	})
	if err != nil {
		t.Fatal(err)
	}
	capabilities := CapabilitiesOf(conn)
	if !capabilities.Read || !capabilities.Write {
		t.Fatalf("unexpected google gmail capabilities: %+v", capabilities)
	}
}
