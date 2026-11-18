package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/mrchypark/conductor/internal/callbacks"
	"github.com/mrchypark/conductor/internal/connections"
	ampconnectors "github.com/mrchypark/conductor/internal/connectors"
	"github.com/mrchypark/conductor/internal/executions"
	"github.com/mrchypark/conductor/internal/handlers"
	"github.com/mrchypark/conductor/internal/persistence"
	"github.com/mrchypark/conductor/internal/storage"
)

func loadDynamicHandlers(ctx context.Context, store *storage.Store, registry *handlers.Registry) error {
	if store == nil {
		return nil
	}
	records, err := store.ListDynamicHandlers(ctx)
	if err != nil {
		return err
	}
	for _, record := range records {
		sourceHash := fmt.Sprintf("%x", sha256.Sum256(record.Source))
		if sourceHash != record.SourceHash {
			return fmt.Errorf("dynamic handler %s@%s source hash mismatch", record.HandlerKey, record.Version)
		}
		var manifest handlers.Manifest
		if err := json.Unmarshal(record.ManifestJSON, &manifest); err != nil {
			return fmt.Errorf("dynamic handler %s@%s manifest: %w", record.HandlerKey, record.Version, err)
		}
		if manifest.Key != record.HandlerKey || manifest.Version != record.Version {
			return fmt.Errorf("dynamic handler %s@%s manifest identity mismatch", record.HandlerKey, record.Version)
		}
		handler, err := handlers.NewKavunHandler(handlers.KavunSpec{
			Manifest: manifest, Source: record.Source, AllowedModules: record.AllowedModules,
			MaxFrames: record.MaxFrames, MaxStack: record.MaxStack, Bytecode: record.Bytecode,
			BytecodeHash: record.BytecodeHash, SourceHash: record.SourceHash, BytecodeSourceHash: record.BytecodeSourceHash, RuntimeABI: record.RuntimeABI,
		})
		if err != nil {
			return fmt.Errorf("dynamic handler %s@%s: %w", record.HandlerKey, record.Version, err)
		}
		bytecode, bytecodeHash, runtimeABI, err := handler.Bytecode()
		if err != nil {
			return fmt.Errorf("dynamic handler %s@%s bytecode: %w", record.HandlerKey, record.Version, err)
		}
		if handler.BytecodeNeedsRefresh() || record.BytecodeHash != bytecodeHash || record.RuntimeABI != runtimeABI {
			if err := store.SaveDynamicHandlerBytecode(ctx, record.HandlerKey, record.Version, bytecode, bytecodeHash, record.SourceHash, runtimeABI); err != nil {
				return fmt.Errorf("dynamic handler %s@%s bytecode cache: %w", record.HandlerKey, record.Version, err)
			}
		}
		if err := registry.Register(handler); err != nil {
			return err
		}
	}
	return nil
}

func loadWASMHandlers(ctx context.Context, store *storage.Store, registry *handlers.Registry, runtime *handlers.WASMRuntime, fetch wasmArtifactFetcher) error {
	if store == nil || runtime == nil || fetch == nil {
		return nil
	}
	records, err := store.ListWASMHandlers(ctx)
	if err != nil {
		return err
	}
	for _, record := range records {
		if record.RuntimeABI != handlers.WASMRuntimeABI {
			return fmt.Errorf("wasm handler %s@%s uses unsupported ABI %q", record.HandlerKey, record.Version, record.RuntimeABI)
		}
		var manifest handlers.Manifest
		if err := json.Unmarshal(record.ManifestJSON, &manifest); err != nil {
			return fmt.Errorf("wasm handler %s@%s manifest: %w", record.HandlerKey, record.Version, err)
		}
		if manifest.Key != record.HandlerKey || manifest.Version != record.Version {
			return fmt.Errorf("wasm handler %s@%s manifest identity mismatch", record.HandlerKey, record.Version)
		}
		record := record
		loader := func(loadCtx context.Context) ([]byte, error) {
			artifact, err := fetch(loadCtx, record.ArtifactRef)
			if err != nil {
				return nil, err
			}
			if artifact.ManifestDigest != record.ManifestDigest || artifact.BlobDigest != record.BlobDigest || artifact.Size != record.ArtifactSize {
				return nil, &handlers.Error{Class: "wasm_invalid", Message: "wasm artifact does not match registered identity"}
			}
			return artifact.Bytes, nil
		}
		handler, err := runtime.NewLazyHandler(handlers.WASMSpec{Manifest: manifest, Load: loader})
		if err != nil {
			return fmt.Errorf("wasm handler %s@%s: %w", record.HandlerKey, record.Version, err)
		}
		if err := registry.Register(handler); err != nil {
			return err
		}
	}
	return nil
}

func main() {
	addr := flag.String("addr", envOr("CONDUCTOR_ADDR", ":8080"), "HTTP listen address")
	dsn := flag.String("db", envOr("CONDUCTOR_DB", "conductor.db"), "Rhiza data directory")
	flag.Parse()

	ctx := context.Background()
	config, err := rhizaConfig(*dsn)
	if err != nil {
		log.Fatal(err)
	}
	store, err := storage.Open(ctx, config)
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()
	registry := handlers.NewRegistry()
	wasmRuntime := handlers.NewWASMRuntime(ctx)
	defer wasmRuntime.Close(ctx)
	wasmFetch := newOCIWASMFetcher(
		os.Getenv("CONDUCTOR_WASM_REGISTRIES"),
		os.Getenv("CONDUCTOR_WASM_REGISTRY_USERNAME"),
		os.Getenv("CONDUCTOR_WASM_REGISTRY_PASSWORD"),
		os.Getenv("CONDUCTOR_WASM_REGISTRY_TOKEN"),
		strings.EqualFold(os.Getenv("CONDUCTOR_WASM_REGISTRY_PLAIN_HTTP"), "true"),
	)
	echo, err := handlers.NewFuncHandler(handlers.Manifest{Key: "payload.echo", Version: "1", DefaultTimeout: 30, Idempotency: "pure", ResourceClass: "small"}, func(_ context.Context, request handlers.Request) (handlers.Result, error) {
		return handlers.Result{Output: append([]byte(nil), request.Input...)}, nil
	})
	if err != nil {
		log.Fatal(err)
	}
	if err := registry.Register(echo); err != nil {
		log.Fatal(err)
	}
	if err := loadDynamicHandlers(ctx, store, registry); err != nil {
		log.Fatal(err)
	}
	if err := loadWASMHandlers(ctx, store, registry, wasmRuntime, wasmFetch); err != nil {
		log.Fatal(err)
	}
	service, err := executions.NewService(store, registry)
	if err != nil {
		log.Fatal(err)
	}
	service.SetCallbackTarget(os.Getenv("CONDUCTOR_CALLBACK_URL"))
	callbackDelivery := &callbacks.Delivery{Store: store, Token: os.Getenv("CONDUCTOR_CALLBACK_TOKEN")}
	workerCtx, stopWorker := context.WithCancel(ctx)
	defer stopWorker()
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-workerCtx.Done():
				return
			case <-ticker.C:
				if err := callbackDelivery.RunOnce(workerCtx); err != nil {
					log.Printf("callback delivery: %v", err)
				}
			}
		}
	}()
	catalog, err := ampconnectors.LoadCatalog()
	if err != nil {
		log.Fatal(fmt.Errorf("load connector catalog: %w", err))
	}
	if base := os.Getenv("BUSINESS_SERVICE_BASE_URL"); base != "" {
		catalog.SetBusinessClient(&ampconnectors.BusinessClient{BaseURL: base, Token: os.Getenv("BUSINESS_SERVICE_TOKEN")})
	}
	var connectionStore *connections.Store
	if key := os.Getenv("CONDUCTOR_MASTER_KEY"); key != "" {
		connectionStore, err = connections.NewStore(ctx, store.Persistence(), []byte(key))
		if err != nil {
			log.Fatal(err)
		}
	}
	resolver := connectorCredentialsResolver(connectionStore, catalog)
	connectorHandler, err := catalog.NewInvokeHandlerScoped("2", nil, resolver)
	if err != nil {
		log.Fatal(err)
	}
	if err := registry.Register(connectorHandler); err != nil {
		log.Fatal(err)
	}
	if err := loadCredentialProviderHandler(os.Getenv("CONDUCTOR_CREDENTIAL_PROVIDER_CONFIG"), os.Getenv("CONDUCTOR_SERVICE_TOKEN"), registry, nil, nil); err != nil {
		log.Fatal(err)
	}
	for _, manifest := range registry.Manifests() {
		if err := store.SaveHandlerManifest(ctx, manifest.Key, manifest.Version, manifest); err != nil {
			log.Fatal(err)
		}
	}
	for _, provider := range catalog.Names() {
		if info, err := catalog.Info(provider); err == nil {
			if err := store.SaveConnectorManifest(ctx, provider, info); err != nil {
				log.Fatal(err)
			}
		}
	}
	// Register configured handlers before claiming durable work after a restart.
	startExecutionProcessing(workerCtx, service, store, os.Getenv("CONDUCTOR_WORKER_ID"), 250*time.Millisecond)
	log.Printf("conductor listening on %s with %d connector providers", *addr, len(catalog.Names()))
	srv := newServer(service, registry, catalog)
	srv.store = store
	srv.oauthStore = connectionStore
	srv.wasmRuntime = wasmRuntime
	srv.wasmFetch = wasmFetch
	httpServer := &http.Server{
		Addr:              *addr,
		Handler:           srv,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      2 * time.Minute,
		IdleTimeout:       2 * time.Minute,
	}
	signalCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	serverErr := make(chan error, 1)
	go func() {
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
		close(serverErr)
	}()
	select {
	case err := <-serverErr:
		if err != nil {
			log.Fatal(err)
		}
	case <-signalCtx.Done():
		shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 15*time.Second)
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			log.Printf("http shutdown: %v", err)
		}
		cancelShutdown()
	}
}

func startExecutionProcessing(ctx context.Context, service *executions.Service, store *storage.Store, workerID string, interval time.Duration) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		var workerDone chan struct{}
		if workerID != "" {
			workerDone = make(chan struct{})
			go func() {
				defer close(workerDone)
				if err := service.RunWorker(ctx, workerID, interval, time.Minute); err != nil && !errors.Is(err, context.Canceled) {
					log.Printf("execution worker: %v", err)
				}
			}()
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			if err := store.RecoverExpiredInlineLeases(ctx, time.Now().UTC()); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("execution recovery: %v", err)
			}
			select {
			case <-ctx.Done():
				if workerDone != nil {
					<-workerDone
				}
				return
			case <-ticker.C:
			}
		}
	}()
	return done
}

func rhizaConfig(dataDir string) (persistence.Config, error) {
	config := persistence.Config{
		ClusterID:            envOr("CONDUCTOR_RHIZA_CLUSTER_ID", "conductor"),
		NodeID:               envOr("CONDUCTOR_RHIZA_NODE_ID", "n1"),
		DataDir:              dataDir,
		BindAddr:             envOr("CONDUCTOR_RHIZA_BIND_ADDR", "127.0.0.1:0"),
		PeerAddr:             envOr("CONDUCTOR_RHIZA_PEER_ADDR", "127.0.0.1:0"),
		AdminToken:           os.Getenv("CONDUCTOR_RHIZA_ADMIN_TOKEN"),
		PeerToken:            os.Getenv("CONDUCTOR_RHIZA_PEER_TOKEN"),
		ObjStoreProvider:     os.Getenv("CONDUCTOR_RHIZA_S3_PROVIDER"),
		ObjStoreEndpoint:     os.Getenv("CONDUCTOR_RHIZA_S3_ENDPOINT"),
		ObjStoreBucket:       os.Getenv("CONDUCTOR_RHIZA_S3_BUCKET"),
		ObjStorePrefix:       os.Getenv("CONDUCTOR_RHIZA_S3_PREFIX"),
		ObjStoreRegion:       os.Getenv("CONDUCTOR_RHIZA_S3_REGION"),
		ObjStoreAccessKey:    os.Getenv("CONDUCTOR_RHIZA_S3_ACCESS_KEY"),
		ObjStoreSecretKey:    os.Getenv("CONDUCTOR_RHIZA_S3_SECRET_KEY"),
		ObjStoreSessionToken: os.Getenv("CONDUCTOR_RHIZA_S3_SESSION_TOKEN"),
	}
	if value := os.Getenv("CONDUCTOR_RHIZA_S3_INSECURE"); value != "" {
		insecure, err := strconv.ParseBool(value)
		if err != nil {
			return persistence.Config{}, fmt.Errorf("parse CONDUCTOR_RHIZA_S3_INSECURE: %w", err)
		}
		config.ObjStoreInsecure = insecure
	}
	if config.ObjStoreEndpoint != "" && config.ObjStoreProvider == "" {
		config.ObjStoreProvider = "s3"
	}
	if raw := os.Getenv("CONDUCTOR_RHIZA_MEMBERS"); raw != "" {
		if err := json.Unmarshal([]byte(raw), &config.Members); err != nil {
			return persistence.Config{}, fmt.Errorf("parse CONDUCTOR_RHIZA_MEMBERS: %w", err)
		}
	}
	return config, nil
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
