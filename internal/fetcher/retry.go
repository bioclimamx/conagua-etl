package fetcher

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/url"
	"time"
)

// RetryPolicy is the exponential-backoff configuration for transient
// failures. Applied in addition to the global rate limiter.
//
// The JSON tags are load-bearing: the marshalled policy is persisted into
// the pull ledger (_progress.json) alongside the RateConfig.
type RetryPolicy struct {
	// MaxAttempts is the total number of attempts, including the first one.
	// MaxAttempts <= 1 disables retry.
	MaxAttempts int `json:"max_attempts"`

	// BaseDelay is the sleep before the 2nd attempt. Each subsequent retry
	// doubles it, up to MaxDelay.
	BaseDelay time.Duration `json:"base_delay"`
	MaxDelay  time.Duration `json:"max_delay"`
}

// DefaultRetryPolicy returns sensible defaults: 3 attempts total, starting at
// 1 s, capped at 30 s.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts: 3,
		BaseDelay:   1 * time.Second,
		MaxDelay:    30 * time.Second,
	}
}

// Backoff returns the sleep *before* the given attempt. attempt is 1-indexed;
// Backoff(1) is always 0 (no pre-sleep for the first try).
func (p RetryPolicy) Backoff(attempt int) time.Duration {
	if attempt <= 1 {
		return 0
	}
	shift := min(attempt-2, 30) // saturate before int overflow / astronomical waits
	d := p.BaseDelay << shift
	if d <= 0 || d > p.MaxDelay { // d <= 0 guards against sign flip on overflow
		d = p.MaxDelay
	}
	return d
}

// isRetryable classifies the outcome of one HTTP attempt.
//   - Outer context cancelled / expired: NOT retryable (user shutdown).
//   - Transport errors that look transient (timeouts, connection resets):
//     retryable. Crucially, a context.DeadlineExceeded on the error is a
//     *per-request* timeout (HTTP client's own deadline) when the outer
//     ctx is still alive — that's a transport failure and MUST retry.
//     Conflating it with the outer context's deadline would turn a
//     transient timeout into a terminal error.
//   - HTTP 5xx and 429: retryable.
//   - HTTP 404 and other 4xx: NOT retryable (stable answer from server).
func isRetryable(ctx context.Context, status int, err error) bool {
	// Outer run shutting down — stop retrying regardless of what the
	// err/status was. The next attempt would just fail the same way
	// the moment it hit the limiter or the HTTP client.
	if ctx != nil && ctx.Err() != nil {
		return false
	}
	if err != nil {
		// net.Error (Timeout / Temporary), *url.Error (wraps the above),
		// connection resets, RST, "connection broken", per-request
		// context.DeadlineExceeded — all transient transport failures.
		if _, ok := errors.AsType[net.Error](err); ok {
			return true
		}
		if _, ok := errors.AsType[*url.Error](err); ok {
			return true
		}
		// Unknown error class — be conservative and retry; the retry
		// cap and the outer-ctx check keep runaway loops in check.
		return true
	}
	if status >= 500 && status < 600 {
		return true
	}
	if status == http.StatusTooManyRequests {
		return true
	}
	return false
}
