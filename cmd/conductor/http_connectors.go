package main

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/amp-labs/connectors/providers"
)

type customConnectorRequest struct {
	Name        string                    `json:"name"`
	DisplayName string                    `json:"display_name,omitempty"`
	BaseURL     string                    `json:"base_url"`
	AuthType    providers.AuthType        `json:"auth_type"`
	ApiKeyOpts  *providers.ApiKeyOpts     `json:"api_key_opts,omitempty"`
	BasicOpts   *providers.BasicAuthOpts  `json:"basic_opts,omitempty"`
	Oauth2Opts  *providers.Oauth2Opts     `json:"oauth2_opts,omitempty"`
	CustomOpts  *providers.CustomAuthOpts `json:"custom_opts,omitempty"`
	Support     providers.Support         `json:"support,omitempty"`
}

func (s *server) listConnectors(w http.ResponseWriter, _ *http.Request, workspaceID string) {
	if s.catalog == nil {
		writeError(w, http.StatusServiceUnavailable, "connector catalog unavailable")
		return
	}
	writeJSON(w, http.StatusOK, s.catalog.NamesForWorkspace(workspaceID))
}

func (s *server) getConnector(w http.ResponseWriter, _ *http.Request, workspaceID, provider string) {
	if provider == "" || strings.Contains(provider, "/") || s.catalog == nil {
		writeError(w, http.StatusNotFound, "connector not found")
		return
	}
	info, err := s.catalog.InfoForWorkspace(workspaceID, provider)
	if err != nil {
		writeError(w, http.StatusNotFound, "connector not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"name":         info.Name,
		"display_name": info.DisplayName,
		"base_url":     info.BaseURL,
		"auth_type":    info.AuthType,
		"support":      info.Support,
	})
}

func (s *server) registerConnector(w http.ResponseWriter, r *http.Request, workspaceID string) {
	if s.catalog == nil {
		writeError(w, http.StatusServiceUnavailable, "connector catalog unavailable")
		return
	}
	var req customConnectorRequest
	if err := decodeBody(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid connector registration request")
		return
	}
	if req.Name == "" || req.BaseURL == "" {
		writeError(w, http.StatusBadRequest, "name and base_url are required")
		return
	}
	info := providers.ProviderInfo{
		Name:        req.Name,
		DisplayName: req.DisplayName,
		BaseURL:     req.BaseURL,
		AuthType:    req.AuthType,
		ApiKeyOpts:  req.ApiKeyOpts,
		BasicOpts:   req.BasicOpts,
		Oauth2Opts:  req.Oauth2Opts,
		CustomOpts:  req.CustomOpts,
		Support:     req.Support,
	}
	if err := s.catalog.RegisterCustom(workspaceID, req.Name, info, s.allowInsecureCustom); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if s.store != nil {
		infoJSON, err := json.Marshal(info)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to encode connector")
			return
		}
		if err := s.store.SaveCustomConnector(r.Context(), workspaceID, req.Name, infoJSON); err != nil {
			writeError(w, http.StatusInternalServerError, "failed to store connector")
			return
		}
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"workspace_id": workspaceID,
		"name":         req.Name,
		"base_url":     req.BaseURL,
		"auth_type":    info.AuthType,
	})
}
