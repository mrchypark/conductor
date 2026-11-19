package storage

import (
	"context"
	"errors"
	"time"
)

type CustomConnectorRecord struct {
	WorkspaceID      string
	ProviderKey      string
	ProviderInfoJSON []byte
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

func (s *Store) SaveCustomConnector(ctx context.Context, workspaceID, providerKey string, infoJSON []byte) error {
	if workspaceID == "" || providerKey == "" || len(infoJSON) == 0 {
		return errors.New("workspace_id, provider_key, and provider_info_json are required")
	}
	t := now()
	_, err := s.db.Mutate(ctx, "save custom connector",
		stm(`INSERT INTO custom_connectors(workspace_id,provider_key,provider_info_json,created_at,updated_at) VALUES(?,?,?,?,?) ON CONFLICT(workspace_id,provider_key) DO UPDATE SET provider_info_json=excluded.provider_info_json,updated_at=excluded.updated_at`, workspaceID, providerKey, infoJSON, t, t),
	)
	return err
}

func (s *Store) GetCustomConnector(ctx context.Context, workspaceID, providerKey string) (CustomConnectorRecord, error) {
	rows, err := s.db.Query(ctx, `SELECT provider_info_json,created_at,updated_at FROM custom_connectors WHERE workspace_id=? AND provider_key=?`, workspaceID, providerKey)
	if err != nil {
		return CustomConnectorRecord{}, err
	}
	if len(rows) == 0 {
		return CustomConnectorRecord{}, ErrNotFound
	}
	x := rows[0]
	return CustomConnectorRecord{
		WorkspaceID:      workspaceID,
		ProviderKey:      providerKey,
		ProviderInfoJSON: blob(x[0]),
		CreatedAt:        pt(str(x[1])),
		UpdatedAt:        pt(str(x[2])),
	}, nil
}

func (s *Store) ListCustomConnectors(ctx context.Context, workspaceID string) ([]CustomConnectorRecord, error) {
	rows, err := s.db.Query(ctx, `SELECT provider_key,provider_info_json,created_at,updated_at FROM custom_connectors WHERE workspace_id=? ORDER BY provider_key`, workspaceID)
	if err != nil {
		return nil, err
	}
	out := make([]CustomConnectorRecord, 0, len(rows))
	for _, x := range rows {
		out = append(out, CustomConnectorRecord{
			WorkspaceID:      workspaceID,
			ProviderKey:      str(x[0]),
			ProviderInfoJSON: blob(x[1]),
			CreatedAt:        pt(str(x[2])),
			UpdatedAt:        pt(str(x[3])),
		})
	}
	return out, nil
}

func (s *Store) ListAllCustomConnectors(ctx context.Context) ([]CustomConnectorRecord, error) {
	rows, err := s.db.Query(ctx, `SELECT workspace_id,provider_key,provider_info_json,created_at,updated_at FROM custom_connectors ORDER BY workspace_id,provider_key`)
	if err != nil {
		return nil, err
	}
	out := make([]CustomConnectorRecord, 0, len(rows))
	for _, x := range rows {
		out = append(out, CustomConnectorRecord{
			WorkspaceID:      str(x[0]),
			ProviderKey:      str(x[1]),
			ProviderInfoJSON: blob(x[2]),
			CreatedAt:        pt(str(x[3])),
			UpdatedAt:        pt(str(x[4])),
		})
	}
	return out, nil
}
