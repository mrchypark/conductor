package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/mrchypark/conductor/internal/connectors"
	"github.com/mrchypark/conductor/internal/handlers"
)

// Only operator configuration can install grants. Execution input never supplies
// a destination, token file, or credential-delivery binding.
func loadCredentialProviderHandler(path, serviceToken string, registry *handlers.Registry, authority, provider *http.Client) error {
	if path == "" {
		return nil
	}
	if strings.TrimSpace(serviceToken) == "" {
		return errors.New("external credential provider execution requires CONDUCTOR_SERVICE_TOKEN")
	}
	f, err := os.Open(path)
	if err != nil {
		return errors.New("cannot open external credential provider configuration")
	}
	defer f.Close()
	body, err := io.ReadAll(io.LimitReader(f, (1<<20)+1))
	if err != nil || len(body) > 1<<20 {
		return errors.New("external credential provider configuration exceeds size limit or cannot be read")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var config connectors.CredentialProviderConfig
	if err := decoder.Decode(&config); err != nil {
		return errors.New("invalid external credential provider configuration")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return errors.New("external credential provider configuration must contain one JSON object")
	}
	v2, v3, err := connectors.NewCredentialProviderHandlers(config, authority, provider)
	if err != nil {
		return err
	}
	fileBacked := 0
	for _, binding := range config.Bindings {
		if binding.TokenFile != "" {
			fileBacked++
		}
	}
	if fileBacked != 0 && fileBacked != len(config.Bindings) {
		return errors.New("credential provider token files must be configured for all bindings or none")
	}
	if fileBacked > 0 {
		v1, err := connectors.NewCredentialProviderHandler(config, authority, provider)
		if err != nil {
			return err
		}
		if err := registry.Register(v1); err != nil {
			return err
		}
	}
	if err := registry.Register(v2); err != nil {
		return err
	}
	return registry.Register(v3)
}
