package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/mrchypark/conductor/internal/handlers"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2"
	"oras.land/oras-go/v2/content"
	"oras.land/oras-go/v2/registry"
	"oras.land/oras-go/v2/registry/remote"
	"oras.land/oras-go/v2/registry/remote/auth"
	"oras.land/oras-go/v2/registry/remote/retry"
)

const (
	wasmArtifactType = "application/vnd.conductor.function.v1"
	wasmLayerType    = "application/vnd.conductor.function.wasm.v1+wasm"
)

type wasmArtifact struct {
	Bytes          []byte
	ManifestDigest string
	BlobDigest     string
	Size           int64
}

type wasmArtifactFetcher func(context.Context, string) (wasmArtifact, error)

func newOCIWASMFetcher(allowedRegistries, username, password, token string, plainHTTP bool) wasmArtifactFetcher {
	allowed := make(map[string]bool)
	for _, host := range strings.Split(allowedRegistries, ",") {
		if host = strings.ToLower(strings.TrimSpace(host)); host != "" {
			allowed[host] = true
		}
	}
	return func(ctx context.Context, artifactRef string) (wasmArtifact, error) {
		ref, err := registry.ParseReference(artifactRef)
		if err != nil {
			return wasmArtifact{}, fmt.Errorf("invalid OCI artifact reference: %w", err)
		}
		if !allowed[strings.ToLower(ref.Registry)] {
			return wasmArtifact{}, fmt.Errorf("OCI registry %q is not allowed", ref.Registry)
		}
		manifestDigest, err := digest.Parse(ref.Reference)
		if err != nil || manifestDigest.Algorithm() != digest.SHA256 {
			return wasmArtifact{}, errors.New("OCI artifact reference must be pinned by sha256 digest")
		}
		repo, err := remote.NewRepository(ref.Registry + "/" + ref.Repository)
		if err != nil {
			return wasmArtifact{}, err
		}
		repo.PlainHTTP = plainHTTP
		repo.Client = &auth.Client{
			Client: retry.DefaultClient,
			Cache:  auth.NewCache(),
			Credential: func(_ context.Context, hostport string) (auth.Credential, error) {
				if !strings.EqualFold(hostport, ref.Registry) {
					return auth.EmptyCredential, nil
				}
				return auth.Credential{Username: username, Password: password, AccessToken: token}, nil
			},
		}
		desc, manifestBytes, err := oras.FetchBytes(ctx, repo, manifestDigest.String(), oras.FetchBytesOptions{MaxBytes: 1 << 20})
		if err != nil {
			return wasmArtifact{}, fmt.Errorf("fetch OCI manifest: %w", err)
		}
		if desc.Digest != manifestDigest {
			return wasmArtifact{}, errors.New("OCI manifest digest mismatch")
		}
		if desc.MediaType != ocispec.MediaTypeImageManifest {
			return wasmArtifact{}, fmt.Errorf("OCI artifact manifest must have media type %s", ocispec.MediaTypeImageManifest)
		}
		var manifest ocispec.Manifest
		if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
			return wasmArtifact{}, fmt.Errorf("decode OCI manifest: %w", err)
		}
		if manifest.ArtifactType != wasmArtifactType || len(manifest.Layers) != 1 {
			return wasmArtifact{}, fmt.Errorf("OCI artifact must have type %s and exactly one layer", wasmArtifactType)
		}
		layer := manifest.Layers[0]
		if layer.MediaType != wasmLayerType {
			return wasmArtifact{}, fmt.Errorf("OCI layer must have media type %s", wasmLayerType)
		}
		if layer.Size <= 0 || layer.Size > handlers.MaxWASMArtifactBytes {
			return wasmArtifact{}, fmt.Errorf("WASM layer size must be between 1 and %d bytes", handlers.MaxWASMArtifactBytes)
		}
		reader, err := repo.Fetch(ctx, layer)
		if err != nil {
			return wasmArtifact{}, fmt.Errorf("fetch WASM layer: %w", err)
		}
		defer reader.Close()
		wasm, err := content.ReadAll(reader, layer)
		if err != nil {
			return wasmArtifact{}, fmt.Errorf("verify WASM layer: %w", err)
		}
		if int64(len(wasm)) != layer.Size {
			return wasmArtifact{}, errors.New("WASM layer digest or size mismatch")
		}
		return wasmArtifact{Bytes: wasm, ManifestDigest: manifestDigest.String(), BlobDigest: layer.Digest.String(), Size: layer.Size}, nil
	}
}
