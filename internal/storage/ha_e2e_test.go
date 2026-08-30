package storage_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/conductor/internal/callbacks"
	"github.com/mrchypark/conductor/internal/connections"
	"github.com/mrchypark/conductor/internal/persistence"
	"github.com/mrchypark/conductor/internal/storage"
	"github.com/mrchypark/rhiza"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"golang.org/x/oauth2"
)

const haMasterKey = "01234567890123456789012345678901"

type haNodeConfig struct {
	Rhiza       persistence.Config `json:"rhiza"`
	ControlAddr string             `json:"control_addr"`
	CallbackURL string             `json:"callback_url"`
}

type haNode struct {
	config haNodeConfig
	cmd    *exec.Cmd
	log    bytes.Buffer
}

func TestThreeProcessRhizaHA(t *testing.T) {
	if testing.Short() {
		t.Skip("three-process HA E2E")
	}
	s3Endpoint := os.Getenv("CONDUCTOR_HA_E2E_S3_ENDPOINT")
	if s3Endpoint == "" {
		t.Skip("CONDUCTOR_HA_E2E_S3_ENDPOINT is required for Rhiza multi-node HA")
	}
	s3Bucket := envOr("CONDUCTOR_HA_E2E_S3_BUCKET", "conductor-ha-e2e")
	s3AccessKey := envOr("CONDUCTOR_HA_E2E_S3_ACCESS_KEY", "minioadmin")
	s3SecretKey := envOr("CONDUCTOR_HA_E2E_S3_SECRET_KEY", "minioadmin")
	var callbackCount atomic.Int32
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callbackCount.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer callbackServer.Close()
	var exchangeCount atomic.Int32
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		exchangeCount.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"access_token":"ha-access","token_type":"Bearer","expires_in":3600}`)
	}))
	defer tokenServer.Close()

	peerAddrs := []string{freeUDPAddr(t), freeUDPAddr(t), freeUDPAddr(t)}
	members := make([]rhiza.Member, 3)
	for i := range members {
		members[i] = rhiza.Member{ID: quepaxa.NodeID(fmt.Sprintf("n%d", i+1)), PeerURL: "quic://" + peerAddrs[i], Token: "ha-secret"}
	}
	root := t.TempDir()
	nodes := make([]*haNode, 3)
	for i := range nodes {
		nodes[i] = &haNode{config: haNodeConfig{
			Rhiza: persistence.Config{
				ClusterID: "conductor-ha-e2e", NodeID: fmt.Sprintf("n%d", i+1), DataDir: filepath.Join(root, fmt.Sprintf("n%d", i+1)), PeerAddr: peerAddrs[i], AdminToken: "ha-secret", Members: members,
				ObjStoreProvider: "s3", ObjStoreEndpoint: s3Endpoint, ObjStoreBucket: s3Bucket, ObjStoreAccessKey: s3AccessKey, ObjStoreSecretKey: s3SecretKey, ObjStoreRegion: "us-east-1", ObjStoreInsecure: true,
			},
			ControlAddr: freeTCPAddr(t), CallbackURL: callbackServer.URL,
		}}
		startHANode(t, nodes[i])
	}
	t.Cleanup(func() {
		for _, node := range nodes {
			stopHANode(node)
		}
	})
	for _, node := range nodes {
		waitHAReady(t, node)
	}

	postOK(t, nodes[0], "/create", map[string]any{"request_id": "lease-race", "execution_id": "exec_lease-race"}, nil)
	lease := raceLease(t, nodes[0], nodes[1], "exec_lease-race")
	postOK(t, lease.node, "/complete", map[string]any{"execution_id": "exec_lease-race", "lease_id": lease.record.ID, "fencing_token": lease.record.FencingToken}, nil)
	raceDelivery(t, nodes[0], nodes[1])
	if callbackCount.Load() != 1 {
		t.Fatalf("callback delivered %d times", callbackCount.Load())
	}

	var begin connections.BeginResult
	postOK(t, nodes[0], "/oauth/begin", map[string]any{"token_url": tokenServer.URL}, &begin)
	winners := raceOAuthComplete(t, nodes[0], nodes[1], begin, tokenServer.URL)
	if winners != 1 || exchangeCount.Load() != 1 {
		t.Fatalf("oauth winners/exchanges=%d/%d", winners, exchangeCount.Load())
	}

	stopHANode(nodes[2])
	postOK(t, nodes[0], "/create", map[string]any{"request_id": "peer-loss", "execution_id": "exec_peer-loss"}, nil)
	lostPeerLease := raceLease(t, nodes[0], nodes[1], "exec_peer-loss")
	postOK(t, lostPeerLease.node, "/complete", map[string]any{"execution_id": "exec_peer-loss", "lease_id": lostPeerLease.record.ID, "fencing_token": lostPeerLease.record.FencingToken}, nil)
	raceDelivery(t, nodes[0], nodes[1])
	if callbackCount.Load() != 2 {
		t.Fatalf("callback during peer loss delivered %d times", callbackCount.Load())
	}

	startHANode(t, nodes[2])
	waitHAReady(t, nodes[2])
	var execution storage.ExecutionRecord
	getOK(t, nodes[2], "/execution?id=exec_peer-loss", &execution)
	if execution.Status != "succeeded" {
		t.Fatalf("restarted peer status=%s", execution.Status)
	}
	var connection connections.Connection
	getOK(t, nodes[2], "/connection?id=conn-ha", &connection)
	if connection.Token == nil || connection.Token.AccessToken != "ha-access" {
		t.Fatalf("restarted peer OAuth state=%+v", connection)
	}
}

// TestRhizaHAHelper is executed only in child OS processes spawned above.
func TestRhizaHAHelper(t *testing.T) {
	if os.Getenv("CONDUCTOR_HA_HELPER") != "1" {
		t.Skip("helper process")
	}
	var config haNodeConfig
	if err := json.Unmarshal([]byte(os.Getenv("CONDUCTOR_HA_CONFIG")), &config); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	store, err := storage.Open(ctx, config.Rhiza)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	store.SetCallbackTarget(config.CallbackURL)
	oauthStore, err := connections.NewStore(ctx, store.Persistence(), []byte(haMasterKey))
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		if err := store.Persistence().Ready(r.Context()); err != nil {
			http.Error(w, err.Error(), 503)
			return
		}
		w.WriteHeader(204)
	})
	mux.HandleFunc("/create", jsonHandler(func(ctx context.Context, input struct {
		RequestID   string `json:"request_id"`
		ExecutionID string `json:"execution_id"`
	}) (any, error) {
		_, err := store.CreateExecution(ctx, storage.ExecutionRequestRecord{ID: input.RequestID, WorkspaceID: "ws-ha", HandlerKey: "ha.handler", HandlerVersion: "1", Input: []byte(`{}`), MaxAttempts: 2}, input.ExecutionID)
		return nil, err
	}))
	mux.HandleFunc("/lease", jsonHandler(func(ctx context.Context, input struct {
		ExecutionID string `json:"execution_id"`
		WorkerID    string `json:"worker_id"`
	}) (any, error) {
		return store.AcquireLease(ctx, input.ExecutionID, input.WorkerID, time.Minute, 1)
	}))
	mux.HandleFunc("/complete", jsonHandler(func(ctx context.Context, input struct {
		ExecutionID  string `json:"execution_id"`
		LeaseID      string `json:"lease_id"`
		FencingToken int64  `json:"fencing_token"`
	}) (any, error) {
		return nil, store.CompleteExecutionWithLease(ctx, input.LeaseID, input.FencingToken, input.ExecutionID, "succeeded", []byte(`{"ok":true}`), "", "", false, 1)
	}))
	mux.HandleFunc("/deliver", jsonHandler(func(ctx context.Context, _ struct{}) (any, error) {
		return nil, (&callbacks.Delivery{Store: store}).RunOnce(ctx)
	}))
	mux.HandleFunc("/execution", func(w http.ResponseWriter, r *http.Request) {
		value, err := store.GetExecution(r.Context(), r.URL.Query().Get("id"))
		writeJSON(w, value, err)
	})
	mux.HandleFunc("/connection", func(w http.ResponseWriter, r *http.Request) {
		value, err := oauthStore.LoadConnection(r.Context(), r.URL.Query().Get("id"))
		writeJSON(w, value, err)
	})
	mux.HandleFunc("/oauth/begin", jsonHandler(func(ctx context.Context, input struct {
		TokenURL string `json:"token_url"`
	}) (any, error) {
		return oauthStore.Begin(ctx, connections.BeginInput{AttemptID: "attempt-ha", Provider: "test", ConnectionID: "conn-ha", WorkspaceID: "ws-ha", RedirectURL: "http://conductor.test/callback", ClientID: "client-ha", Now: time.Now().UTC()}, oauth2.Endpoint{AuthURL: "http://provider.test/auth", TokenURL: input.TokenURL})
	}))
	mux.HandleFunc("/oauth/complete", jsonHandler(func(ctx context.Context, input struct {
		AttemptID string `json:"attempt_id"`
		State     string `json:"state"`
		TokenURL  string `json:"token_url"`
	}) (any, error) {
		return oauthStore.Complete(ctx, oauth2.Config{ClientID: "client-ha", RedirectURL: "http://conductor.test/callback", Endpoint: oauth2.Endpoint{AuthURL: "http://provider.test/auth", TokenURL: input.TokenURL}}, connections.CompleteInput{AttemptID: input.AttemptID, State: input.State, Code: "code-ha", WorkspaceID: "ws-ha", Now: time.Now().UTC()})
	}))
	server := &http.Server{Addr: config.ControlAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		t.Fatal(err)
	}
}

type leaseWinner struct {
	node   *haNode
	record storage.LeaseRecord
}

func raceLease(t *testing.T, a, b *haNode, executionID string) leaseWinner {
	t.Helper()
	nodes := []*haNode{a, b}
	results := make(chan leaseWinner, 2)
	errorsSeen := make(chan error, 2)
	var wg sync.WaitGroup
	for i, node := range nodes {
		wg.Add(1)
		go func(i int, node *haNode) {
			defer wg.Done()
			var lease storage.LeaseRecord
			if err := post(node, "/lease", map[string]any{"execution_id": executionID, "worker_id": fmt.Sprintf("w%d", i+1)}, &lease); err == nil {
				results <- leaseWinner{node, lease}
			} else {
				errorsSeen <- err
			}
		}(i, node)
	}
	wg.Wait()
	close(results)
	close(errorsSeen)
	winners := make([]leaseWinner, 0, 1)
	for winner := range results {
		winners = append(winners, winner)
	}
	if len(winners) != 1 {
		var failures []error
		for err := range errorsSeen {
			failures = append(failures, err)
		}
		t.Fatalf("lease winners=%d errors=%v", len(winners), failures)
	}
	return winners[0]
}
func raceDelivery(t *testing.T, a, b *haNode) {
	t.Helper()
	var wg sync.WaitGroup
	for _, node := range []*haNode{a, b} {
		wg.Add(1)
		go func(node *haNode) { defer wg.Done(); _ = post(node, "/deliver", map[string]any{}, nil) }(node)
	}
	wg.Wait()
	time.Sleep(50 * time.Millisecond)
}
func raceOAuthComplete(t *testing.T, a, b *haNode, begin connections.BeginResult, tokenURL string) int {
	t.Helper()
	var winners atomic.Int32
	var wg sync.WaitGroup
	for _, node := range []*haNode{a, b} {
		wg.Add(1)
		go func(node *haNode) {
			defer wg.Done()
			if post(node, "/oauth/complete", map[string]any{"attempt_id": begin.AttemptID, "state": begin.State, "token_url": tokenURL}, nil) == nil {
				winners.Add(1)
			}
		}(node)
	}
	wg.Wait()
	return int(winners.Load())
}

func startHANode(t *testing.T, node *haNode) {
	t.Helper()
	raw, err := json.Marshal(node.config)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestRhizaHAHelper$", "-test.timeout=0")
	cmd.Env = append(os.Environ(), "CONDUCTOR_HA_HELPER=1", "CONDUCTOR_HA_CONFIG="+string(raw))
	node.log.Reset()
	cmd.Stdout = &node.log
	cmd.Stderr = &node.log
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	node.cmd = cmd
}
func stopHANode(node *haNode) {
	if node == nil || node.cmd == nil || node.cmd.Process == nil {
		return
	}
	_ = node.cmd.Process.Kill()
	_ = node.cmd.Wait()
	node.cmd = nil
}
func waitHAReady(t *testing.T, node *haNode) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if node.cmd != nil && node.cmd.ProcessState != nil {
			t.Fatalf("node exited: %s", node.log.String())
		}
		req, _ := http.NewRequest(http.MethodGet, "http://"+node.config.ControlAddr+"/ready", nil)
		client := http.Client{Timeout: 500 * time.Millisecond}
		if resp, err := client.Do(req); err == nil {
			resp.Body.Close()
			if resp.StatusCode == 204 {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("node not ready: %s", node.log.String())
}
func postOK(t *testing.T, node *haNode, path string, input, output any) {
	t.Helper()
	if err := post(node, path, input, output); err != nil {
		t.Fatal(err)
	}
}
func post(node *haNode, path string, input, output any) error {
	body, _ := json.Marshal(input)
	client := http.Client{Timeout: 10 * time.Second}
	resp, err := client.Post("http://"+node.config.ControlAddr+path, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("%s: %s", resp.Status, string(data))
	}
	if output != nil && len(data) > 0 {
		return json.Unmarshal(data, output)
	}
	return nil
}
func getOK(t *testing.T, node *haNode, path string, output any) {
	t.Helper()
	client := http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get("http://" + node.config.ControlAddr + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("%s: %s", resp.Status, data)
	}
	if err = json.NewDecoder(resp.Body).Decode(output); err != nil {
		t.Fatal(err)
	}
}
func jsonHandler[I any, O any](fn func(context.Context, I) (O, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var input I
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		output, err := fn(r.Context(), input)
		if err != nil {
			http.Error(w, err.Error(), 409)
			return
		}
		if any(output) == nil {
			w.WriteHeader(204)
			return
		}
		_ = json.NewEncoder(w).Encode(output)
	}
}
func writeJSON[T any](w http.ResponseWriter, value T, err error) {
	if err != nil {
		http.Error(w, err.Error(), 404)
		return
	}
	_ = json.NewEncoder(w).Encode(value)
}
func freeTCPAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}
func freeUDPAddr(t *testing.T) string {
	t.Helper()
	l, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.LocalAddr().String()
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
