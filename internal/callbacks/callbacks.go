// Package callbacks delivers terminal runtime callbacks from a durable outbox.
package callbacks

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/mrchypark/conductor/internal/storage"
)

type Delivery struct {
	Store  *storage.Store
	Client *http.Client
	Token  string
	Now    func() time.Time
}

func (d *Delivery) RunOnce(ctx context.Context) error {
	if d == nil || d.Store == nil {
		return fmt.Errorf("callback store is required")
	}
	client := d.Client
	if client == nil {
		client = http.DefaultClient
	}
	now := time.Now
	if d.Now != nil {
		now = d.Now
	}
	if err := d.Store.RecoverCallbackDeliveries(ctx, now().UTC(), 30*time.Second); err != nil {
		return err
	}
	items, err := d.Store.DueCallbackDeliveries(ctx, now().UTC(), 20)
	if err != nil {
		return err
	}
	for _, item := range items {
		requestCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		req, err := http.NewRequestWithContext(requestCtx, http.MethodPost, item.TargetURL, bytes.NewReader(item.Payload))
		if err != nil {
			cancel()
			_ = d.Store.MarkCallbackDelivery(ctx, item.ID, "failed", err.Error(), now())
			continue
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", item.ID)
		if d.Token != "" {
			req.Header.Set("Authorization", "Bearer "+d.Token)
		}
		resp, callErr := client.Do(req)
		cancel()
		if callErr == nil {
			resp.Body.Close()
		}
		if callErr == nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
			if err := d.Store.MarkCallbackDelivery(ctx, item.ID, "delivered", "", now()); err != nil {
				return err
			}
			continue
		}
		message := "callback delivery failed"
		if callErr != nil {
			message = callErr.Error()
		} else {
			message = "http status " + strconv.Itoa(resp.StatusCode)
		}
		attempt := item.AttemptCount + 1
		delay := time.Second * time.Duration(1<<min(attempt, 6))
		status := "retry_wait"
		if callErr == nil && resp.StatusCode >= 400 && resp.StatusCode < 500 && resp.StatusCode != http.StatusTooManyRequests {
			status = "failed"
		}
		if err := d.Store.MarkCallbackDelivery(ctx, item.ID, status, message, now().UTC().Add(delay)); err != nil {
			return err
		}
	}
	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
