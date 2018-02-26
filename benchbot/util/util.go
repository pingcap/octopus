package util

import (
	"context"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
)

// RetryOnError defines a action with retry when "fn" returns error
func RetryOnError(ctx context.Context, retryCount int, interval time.Duration, fn func() error) error {
	var err error
	for i := 0; i < retryCount; i++ {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		err = fn()
		if err == nil {
			break
		}

		log.Errorf("Error: %v, retry", err)
		Sleep(ctx, interval)
	}

	return errors.Trace(err)
}

// Sleep defines special `sleep` with context
func Sleep(ctx context.Context, sleepTime time.Duration) {
	ticker := time.NewTicker(sleepTime)
	defer ticker.Stop()

	select {
	case <-ctx.Done():
		return
	case <-ticker.C:
		return
	}
}
