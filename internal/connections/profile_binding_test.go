package connections

import (
	"context"
	"testing"
	"time"
)

func TestConnectionProfileBindingIsImmutable(t *testing.T) {
	store, _ := newOAuthStore(t)
	ctx := context.Background()
	save := func(id, workspace, provider, key string) error {
		return store.SaveConnection(ctx, id, workspace, provider, nil, nil, map[string]string{"api_key": key}, time.Unix(1700000000, 0))
	}
	for _, id := range []string{"personal", "work"} {
		if err := save(id, "ws", "github", id); err != nil {
			t.Fatal(err)
		}
	}
	for _, target := range [][2]string{{"other", "github"}, {"ws", "slack"}} {
		if err := save("personal", target[0], target[1], "replacement"); err == nil {
			t.Fatal("profile rebinding succeeded")
		}
	}
	for _, id := range []string{"personal", "work"} {
		c, err := store.LoadConnection(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if c.WorkspaceID != "ws" || c.Provider != "github" || c.Metadata["api_key"] != id {
			t.Fatal("profile changed after rejected rebinding")
		}
	}
	if err := save("personal", "ws", "github", "rotated"); err != nil {
		t.Fatal(err)
	}
	a, err := store.LoadConnection(ctx, "personal")
	if err != nil {
		t.Fatal(err)
	}
	b, err := store.LoadConnection(ctx, "work")
	if err != nil {
		t.Fatal(err)
	}
	if a.Metadata["api_key"] != "rotated" || b.Metadata["api_key"] != "work" {
		t.Fatal("rotation did not isolate profiles")
	}
}
