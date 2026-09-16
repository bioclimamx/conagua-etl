package fetcher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/url"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("RetryPolicy", func() {
	Describe("Backoff", func() {
		DescribeTable("doubles from BaseDelay and saturates at MaxDelay (base 1 s, cap 10 s)",
			func(attempt int, want time.Duration) {
				p := RetryPolicy{MaxAttempts: 5, BaseDelay: time.Second, MaxDelay: 10 * time.Second}
				Expect(p.Backoff(attempt)).To(Equal(want))
			},
			Entry("attempt -1 (guard)", -1, time.Duration(0)),
			Entry("attempt 0 (guard)", 0, time.Duration(0)),
			Entry("attempt 1: no pre-sleep", 1, time.Duration(0)),
			Entry("attempt 2: base", 2, time.Second),
			Entry("attempt 3: base ×2", 3, 2*time.Second),
			Entry("attempt 4: base ×4", 4, 4*time.Second),
			Entry("attempt 5: base ×8", 5, 8*time.Second),
			Entry("attempt 6: capped", 6, 10*time.Second),
			Entry("attempt 99: capped", 99, 10*time.Second),
		)

		DescribeTable("the default policy walks 0, 1 s, 2 s, 4 s, … capped at 30 s",
			func(attempt int, want time.Duration) {
				Expect(DefaultRetryPolicy().Backoff(attempt)).To(Equal(want))
			},
			Entry("attempt 1", 1, time.Duration(0)),
			Entry("attempt 2", 2, 1*time.Second),
			Entry("attempt 3", 3, 2*time.Second),
			Entry("attempt 4", 4, 4*time.Second),
			Entry("attempt 5", 5, 8*time.Second),
			Entry("attempt 6", 6, 16*time.Second),
			Entry("attempt 7: 32 s capped to 30 s", 7, 30*time.Second),
			Entry("attempt 50", 50, 30*time.Second),
		)

		It("saturates the shift at 30 so huge attempt numbers stay finite", func() {
			// MaxDelay at the int64 ceiling so the cap can't mask the shift.
			p := RetryPolicy{BaseDelay: time.Nanosecond, MaxDelay: math.MaxInt64}
			Expect(p.Backoff(31)).To(Equal(time.Duration(1) << 29))   // still doubling
			Expect(p.Backoff(32)).To(Equal(time.Duration(1) << 30))   // shift 30 exactly
			Expect(p.Backoff(33)).To(Equal(time.Duration(1) << 30))   // would be 31 — saturated
			Expect(p.Backoff(1000)).To(Equal(time.Duration(1) << 30)) // still shift 30
		})

		It("guards the sign flip when the shifted delay overflows", func() {
			// 30 s << 30 wraps negative in int64; the d <= 0 guard must map
			// it to MaxDelay instead of a zero/negative sleep.
			p := RetryPolicy{BaseDelay: 30 * time.Second, MaxDelay: time.Minute}
			Expect(p.Backoff(40)).To(Equal(time.Minute))
		})
	})

	Describe("ledger serialization", func() {
		// The marshalled policy is persisted into the pull ledger
		// (_progress.json) alongside the RateConfig.
		It("marshals with the pinned ledger keys", func() {
			b, err := json.Marshal(DefaultRetryPolicy())
			Expect(err).NotTo(HaveOccurred())
			Expect(b).To(MatchJSON(`{
				"max_attempts": 3,
				"base_delay": 1000000000,
				"max_delay": 30000000000
			}`))
		})

		It("round-trips every field", func() {
			in := RetryPolicy{MaxAttempts: 7, BaseDelay: 250 * time.Millisecond, MaxDelay: 4 * time.Second}
			b, err := json.Marshal(in)
			Expect(err).NotTo(HaveOccurred())
			var out RetryPolicy
			Expect(json.Unmarshal(b, &out)).To(Succeed())
			Expect(out).To(Equal(in))
		})
	})
})

var _ = Describe("isRetryable", func() {
	liveCtx := func() context.Context { return context.Background() }
	cancelledCtx := func() context.Context {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		return ctx
	}
	expiredCtx := func() context.Context {
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Minute))
		cancel() // release resources; Err stays DeadlineExceeded (first cause wins)
		return ctx
	}

	// wrappedTransportErr mirrors the exact error shape conagua.Client.FetchFile
	// returns: the transport's *url.Error wrapped once with the GET context.
	wrappedTransportErr := fmt.Errorf("GET %s: %w", "https://smn.example/dia01001.txt",
		&url.Error{Op: "Get", URL: "https://smn.example/dia01001.txt", Err: errors.New("connection reset by peer")})

	DescribeTable("classification",
		func(mkCtx func() context.Context, status int, err error, want bool) {
			Expect(isRetryable(mkCtx(), status, err)).To(Equal(want))
		},
		// Stable answers from the server — terminal.
		Entry("200 OK", liveCtx, http.StatusOK, nil, false),
		Entry("404 Not Found", liveCtx, http.StatusNotFound, nil, false),
		Entry("403 Forbidden", liveCtx, http.StatusForbidden, nil, false),
		Entry("400 Bad Request", liveCtx, http.StatusBadRequest, nil, false),
		// Server-side distress — transient.
		Entry("429 Too Many Requests", liveCtx, http.StatusTooManyRequests, nil, true),
		Entry("500", liveCtx, http.StatusInternalServerError, nil, true),
		Entry("502", liveCtx, http.StatusBadGateway, nil, true),
		Entry("503", liveCtx, http.StatusServiceUnavailable, nil, true),
		Entry("599 (non-standard 5xx)", liveCtx, 599, nil, true),
		// Transport failures — transient.
		Entry("net.Error", liveCtx, 0, &net.OpError{Op: "read", Err: errors.New("connection reset")}, true),
		Entry("url.Error as wrapped by conagua.Client", liveCtx, 0, wrappedTransportErr, true),
		Entry("unknown error class (conservative retry)", liveCtx, 0, errors.New("oops"), true),
		// THE load-bearing distinction: context.DeadlineExceeded from the
		// HTTP client's per-request timeout is a transport failure while the
		// outer ctx is alive — it MUST retry. The same error value with the
		// outer ctx expired is shutdown — terminal. Conflating the two turns
		// a transient timeout into a terminal error.
		Entry("per-request deadline, live outer ctx", liveCtx, 0, context.DeadlineExceeded, true),
		Entry("per-request deadline, expired outer ctx", expiredCtx, 0, context.DeadlineExceeded, false),
		// A cancelled outer ctx defeats every otherwise-retryable signal.
		Entry("cancelled ctx: 500", cancelledCtx, http.StatusInternalServerError, nil, false),
		Entry("cancelled ctx: 429", cancelledCtx, http.StatusTooManyRequests, nil, false),
		Entry("cancelled ctx: transient transport error", cancelledCtx, 0,
			&net.OpError{Op: "read", Err: errors.New("connection reset")}, false),
	)
})
