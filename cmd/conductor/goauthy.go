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
func loadGoAuthyHandler(path, serviceToken string, registry *handlers.Registry, authority, provider *http.Client) error {
	if path == "" {
		return nil
	}
	if strings.TrimSpace(serviceToken) == "" {
		return errors.New("GoAuthy execution requires CONDUCTOR_SERVICE_TOKEN")
	}
	f, err := os.Open(path)
	if err != nil {
		return errors.New("cannot open GoAuthy configuration")
	}
	defer f.Close()
	body, err := io.ReadAll(io.LimitReader(f, (1<<20)+1))
	if err != nil || len(body) > 1<<20 {
		return errors.New("GoAuthy configuration exceeds size limit or cannot be read")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var config connectors.GoAuthyConfig
	if err := decoder.Decode(&config); err != nil {
		return errors.New("invalid GoAuthy configuration")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return errors.New("GoAuthy configuration must contain one JSON object")
	}
	handler, err := connectors.NewGoAuthyHandler(config, authority, provider)
	if err != nil {
		return err
	}
	return registry.Register(handler)
}
