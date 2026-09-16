package power_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/fetcher"
	"github.com/bioclimamx/conagua-etl/internal/power"
)

// sampleMonthlyResponse is a minimal POWER monthly body — one
// parameter, three months, plus an ANN convenience key the rollup will
// discard.
const sampleMonthlyResponse = `{
    "properties": { "parameter": {
        "RH2M": { "199101": 65.5, "199102": 64.2, "199103": 60.0, "ANN": 63.0 }
    }},
    "header": { "fill_value": -999 },
    "messages": []
}`

// fixedResponse serves the given status and body for every request and
// counts hits so specs can assert exact attempt counts.
func fixedResponse(status int, body string) (*httptest.Server, *atomic.Int32) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	DeferCleanup(srv.Close)
	return srv, &attempts
}

// monthlyRequest builds a valid monthly-mode FetchRequest; params
// defaults to RH2M alone.
func monthlyRequest(params ...string) power.FetchRequest {
	if len(params) == 0 {
		params = []string{"RH2M"}
	}
	return power.FetchRequest{
		Parameters: params,
		Community:  power.DefaultCommunity,
		StartYear:  1991, EndYear: 2020,
	}
}

// servePowerJSON renders params as a POWER-shaped JSON body with the
// given fill sentinel. No assertions here — it runs on the server
// goroutine, outside Ginkgo's recover scope.
func servePowerJSON(w http.ResponseWriter, params map[string]map[string]float64, fill float64) {
	body := map[string]any{
		"properties": map[string]any{"parameter": params},
		"header":     map[string]any{"fill_value": fill},
		"messages":   []string{},
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(body)
}

var _ = Describe("Client.Fetch", func() {
	It("decodes a monthly response on the first attempt", func() {
		srv, attempts := fixedResponse(http.StatusOK, sampleMonthlyResponse)
		c := power.NewClient(srv.URL, nil)

		req := monthlyRequest()
		req.Lat, req.Lon = 20, -100
		resp, err := c.Fetch(context.Background(), req)
		Expect(err).NotTo(HaveOccurred())
		Expect(attempts.Load()).To(Equal(int32(1)))
		// Round-trip every decoded field, not just presence.
		Expect(resp.Properties.Parameter).To(Equal(map[string]map[string]float64{
			"RH2M": {"199101": 65.5, "199102": 64.2, "199103": 60.0, "ANN": 63.0},
		}))
		Expect(resp.Header.FillValue).To(Equal(-999.0))
		Expect(resp.Messages).To(BeEmpty())
	})

	It("retries transient 503s until success", func() {
		var attempts atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if attempts.Add(1) < 3 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			_, _ = w.Write([]byte(sampleMonthlyResponse))
		}))
		DeferCleanup(srv.Close)

		c := power.NewClient(srv.URL, nil)
		c.BaseBackoff = 5 * time.Millisecond

		resp, err := c.Fetch(context.Background(), monthlyRequest())
		Expect(err).NotTo(HaveOccurred())
		Expect(attempts.Load()).To(Equal(int32(3)), "two transient failures, then success")
		Expect(resp.Properties.Parameter).To(HaveKey("RH2M"))
	})

	It("retries a 429 and succeeds on the next attempt", func() {
		var attempts atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if attempts.Add(1) == 1 {
				w.WriteHeader(http.StatusTooManyRequests)
				return
			}
			_, _ = w.Write([]byte(sampleMonthlyResponse))
		}))
		DeferCleanup(srv.Close)

		c := power.NewClient(srv.URL, nil)
		c.BaseBackoff = time.Millisecond

		resp, err := c.Fetch(context.Background(), monthlyRequest())
		Expect(err).NotTo(HaveOccurred())
		Expect(attempts.Load()).To(Equal(int32(2)), "a 429 is rate pressure — back off and retry")
		Expect(resp.Properties.Parameter).To(HaveKey("RH2M"))
	})

	DescribeTable("treats client errors as terminal",
		func(status int) {
			srv, attempts := fixedResponse(status, `{"error":"bad request"}`)
			c := power.NewClient(srv.URL, nil)
			c.BaseBackoff = time.Millisecond

			_, err := c.Fetch(context.Background(), monthlyRequest())
			Expect(err).To(HaveOccurred())
			Expect(attempts.Load()).To(Equal(int32(1)), "a 4xx is a stable answer — no retry may fire")
		},
		Entry("400 Bad Request", http.StatusBadRequest),
		Entry("404 Not Found", http.StatusNotFound),
	)

	It("surfaces the last error after exhausting attempts", func() {
		srv, attempts := fixedResponse(http.StatusInternalServerError, "boom")
		c := power.NewClient(srv.URL, nil)
		c.MaxAttempts = 3
		c.BaseBackoff = time.Millisecond

		_, err := c.Fetch(context.Background(), monthlyRequest())
		Expect(err).To(MatchError(ContainSubstring("after 3 attempts")))
		Expect(attempts.Load()).To(Equal(int32(3)))
	})

	It("treats outer-context cancellation as terminal", func() {
		ctx, cancel := context.WithCancel(context.Background())
		DeferCleanup(cancel)
		var attempts atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
			attempts.Add(1)
			cancel() // the operator cancels while the request is in flight
			// Never answer: block until the aborted client disconnects, so
			// the cancellation — not a racing server response — is what
			// Fetch deterministically observes.
			<-r.Context().Done()
		}))
		DeferCleanup(srv.Close)

		c := power.NewClient(srv.URL, nil)
		c.BaseBackoff = 50 * time.Millisecond

		_, err := c.Fetch(ctx, monthlyRequest())
		Expect(err).To(MatchError(context.Canceled))
		Expect(attempts.Load()).To(Equal(int32(1)),
			"cancellation must stop the retry loop — a canceled run is not a failure storm")
	})

	It("retries a per-request timeout while the outer ctx is live", func() {
		// THE load-bearing distinction,
		// mirrored from the fetcher classifier but a separate
		// implementation here: the HTTP client's per-request timeout
		// surfaces as a deadline error, yet under a still-live outer ctx
		// it is a transport failure and MUST retry — only outer-ctx
		// cancellation is terminal. Conflating the two turns a transient
		// timeout into a terminal error.
		const clientTimeout = 150 * time.Millisecond
		var attempts atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if attempts.Add(1) == 1 {
				// Stall well past the client timeout so the first attempt
				// deterministically times out; return as soon as the client
				// abandons the request so the spec stays fast.
				select {
				case <-time.After(10 * clientTimeout):
				case <-r.Context().Done():
				}
				return
			}
			_, _ = w.Write([]byte(sampleMonthlyResponse))
		}))
		DeferCleanup(srv.Close)

		c := power.NewClient(srv.URL, nil)
		c.HTTP.Timeout = clientTimeout
		c.BaseBackoff = time.Millisecond

		resp, err := c.Fetch(context.Background(), monthlyRequest())
		Expect(err).NotTo(HaveOccurred())
		Expect(attempts.Load()).To(Equal(int32(2)),
			"a per-request timeout under a live outer ctx is transient — exactly one retry, then success")
		Expect(resp.Properties.Parameter).To(HaveKey("RH2M"))
	})

	It("retries an abruptly severed connection while the outer ctx is live", func() {
		var attempts atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if attempts.Add(1) == 1 {
				// Sever the TCP connection before any bytes are written —
				// the client sees a bare transport error, no HTTP status.
				// The connection is fresh (never reused), so net/http's own
				// idempotent-GET retry cannot fire; the second hit below is
				// driven by power's retry loop alone.
				conn, _, err := w.(http.Hijacker).Hijack()
				if err == nil {
					_ = conn.Close()
				}
				return
			}
			_, _ = w.Write([]byte(sampleMonthlyResponse))
		}))
		DeferCleanup(srv.Close)

		c := power.NewClient(srv.URL, nil)
		c.BaseBackoff = time.Millisecond

		resp, err := c.Fetch(context.Background(), monthlyRequest())
		Expect(err).NotTo(HaveOccurred())
		Expect(attempts.Load()).To(Equal(int32(2)),
			"a network error under a live outer ctx is transient — retry, never fail fast")
		Expect(resp.Properties.Parameter).To(HaveKey("RH2M"))
	})
})

var _ = Describe("Client pacing", func() {
	// pacedConfig runs the limiter at exactly 4 RPS with jitter off, so
	// every sampled delay sits on the 250 ms MinInterval floor and the
	// wall-clock floors below are deterministic. Rests are disabled by
	// zero durations, matching the POWER pacing decision.
	pacedConfig := fetcher.RateConfig{
		TargetRPS:      4,
		MaxRPS:         4,
		JitterFraction: 0,
		MaxInterval:    3 * time.Second,
		RestEveryMin:   50,
		RestEveryMax:   200,
	}

	It("issues requests without delay when no limiter is injected", func() {
		srv, _ := fixedResponse(http.StatusOK, sampleMonthlyResponse)
		c := power.NewClient(srv.URL, nil)

		start := time.Now()
		for range 5 {
			_, err := c.Fetch(context.Background(), monthlyRequest())
			Expect(err).NotTo(HaveOccurred())
		}
		// A 4-RPS-floored limiter would need ≥ 1 s for five requests; an
		// unpaced client must come in under a single interval.
		Expect(time.Since(start)).To(BeNumerically("<", pacedConfig.MinInterval()))
	})

	It("takes one limiter wait per request", func() {
		srv, _ := fixedResponse(http.StatusOK, sampleMonthlyResponse)
		lim, err := fetcher.NewLimiter(pacedConfig, 1)
		Expect(err).NotTo(HaveOccurred())
		c := power.NewClient(srv.URL, lim)

		const n = 3
		start := time.Now()
		for range n {
			_, err := c.Fetch(context.Background(), monthlyRequest())
			Expect(err).NotTo(HaveOccurred())
		}
		Expect(time.Since(start)).To(BeNumerically(">=", (n-1)*pacedConfig.MinInterval()),
			"n paced requests must observe at least n-1 full intervals")
	})

	It("takes a limiter wait for every attempt, including retries", func() {
		var attempts atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if attempts.Add(1) < 3 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			_, _ = w.Write([]byte(sampleMonthlyResponse))
		}))
		DeferCleanup(srv.Close)

		lim, err := fetcher.NewLimiter(pacedConfig, 1)
		Expect(err).NotTo(HaveOccurred())
		c := power.NewClient(srv.URL, lim)
		c.BaseBackoff = time.Millisecond

		start := time.Now()
		_, err = c.Fetch(context.Background(), monthlyRequest())
		Expect(err).NotTo(HaveOccurred())
		Expect(attempts.Load()).To(Equal(int32(3)))
		Expect(time.Since(start)).To(BeNumerically(">=", 2*pacedConfig.MinInterval()),
			"each retry must take its own token even when backoff is negligible")
	})
})

var _ = Describe("BuildURL", func() {
	It("pins the production endpoints and community", func() {
		Expect(power.DefaultEndpointMonthly).To(Equal("https://power.larc.nasa.gov/api/temporal/monthly/point"))
		Expect(power.DefaultEndpointDaily).To(Equal("https://power.larc.nasa.gov/api/temporal/daily/point"))
		Expect(power.DefaultCommunity).To(Equal("AG"))
	})

	It("renders the monthly span deterministically", func() {
		got, err := power.BuildURL(power.DefaultEndpointMonthly, power.FetchRequest{
			Lat: 19.5, Lon: -99.375,
			Parameters: []string{"RH2M", "T2MDEW", "WS2M", "WS10M",
				"ALLSKY_SFC_SW_DWN", "T2M_MAX", "T2M_MIN"},
			Community: "AG",
			StartYear: 1991, EndYear: 2020,
		})
		Expect(err).NotTo(HaveOccurred())

		u, err := url.Parse(got)
		Expect(err).NotTo(HaveOccurred())
		Expect(u.Host).To(Equal("power.larc.nasa.gov"))
		q := u.Query()
		// Parameters stay comma-joined in caller order — the URL a
		// reproducer pastes into POWER must yield numerically identical
		// results to ours.
		Expect(q.Get("parameters")).To(Equal("RH2M,T2MDEW,WS2M,WS10M,ALLSKY_SFC_SW_DWN,T2M_MAX,T2M_MIN"))
		Expect(q.Get("community")).To(Equal("AG"))
		Expect(q.Get("longitude")).To(Equal("-99.375"))
		Expect(q.Get("latitude")).To(Equal("19.5"))
		Expect(q.Get("start")).To(Equal("1991"))
		Expect(q.Get("end")).To(Equal("2020"))
		Expect(q.Get("format")).To(Equal("JSON"))
	})

	It("renders the daily span as 8-digit wire dates", func() {
		got, err := power.BuildURL(power.DefaultEndpointDaily, power.FetchRequest{
			Lat: 21.0, Lon: -89.375,
			Parameters: []string{"T2M"},
			Community:  power.DefaultCommunity,
			StartDate:  "1981-01-01", EndDate: "2026-06-08",
		})
		Expect(err).NotTo(HaveOccurred())

		u, err := url.Parse(got)
		Expect(err).NotTo(HaveOccurred())
		q := u.Query()
		Expect(q.Get("start")).To(Equal("19810101"))
		Expect(q.Get("end")).To(Equal("20260608"))
		Expect(q.Get("latitude")).To(Equal("21"))
		Expect(q.Get("longitude")).To(Equal("-89.375"))
		Expect(q.Get("format")).To(Equal("JSON"))
	})

	It("accepts daily dates already in wire form", func() {
		got, err := power.BuildURL(power.DefaultEndpointDaily, power.FetchRequest{
			Parameters: []string{"T2M"},
			Community:  power.DefaultCommunity,
			StartDate:  "19810101", EndDate: "20260608",
		})
		Expect(err).NotTo(HaveOccurred())

		u, err := url.Parse(got)
		Expect(err).NotTo(HaveOccurred())
		Expect(u.Query().Get("start")).To(Equal("19810101"))
		Expect(u.Query().Get("end")).To(Equal("20260608"))
	})

	It("rejects a malformed daily date", func() {
		_, err := power.BuildURL(power.DefaultEndpointDaily, power.FetchRequest{
			Parameters: []string{"T2M"},
			Community:  power.DefaultCommunity,
			StartDate:  "1981/01/01", EndDate: "2026-06-08",
		})
		Expect(err).To(HaveOccurred())
	})
})

var _ = Describe("FetchRequest.Validate", func() {
	It("accepts a monthly-span request", func() {
		Expect(monthlyRequest().Validate()).To(Succeed())
	})

	It("accepts a daily-span request in ISO form", func() {
		req := power.FetchRequest{
			Parameters: []string{"T2M"},
			Community:  "AG",
			StartDate:  "1981-01-01", EndDate: "2026-06-08",
		}
		Expect(req.Validate()).To(Succeed())
	})

	It("accepts a daily-span request in wire form", func() {
		req := power.FetchRequest{
			Parameters: []string{"T2M"},
			Community:  "AG",
			StartDate:  "19810101", EndDate: "20260608",
		}
		Expect(req.Validate()).To(Succeed())
	})

	DescribeTable("rejects malformed requests",
		func(req power.FetchRequest) {
			Expect(req.Validate()).To(HaveOccurred())
		},
		Entry("no parameters", power.FetchRequest{
			Community: "AG", StartYear: 1991, EndYear: 2020}),
		Entry("no community", power.FetchRequest{
			Parameters: []string{"RH2M"}, StartYear: 1991, EndYear: 2020}),
		Entry("neither span form", power.FetchRequest{
			Parameters: []string{"RH2M"}, Community: "AG"}),
		Entry("inverted year range", power.FetchRequest{
			Parameters: []string{"RH2M"}, Community: "AG", StartYear: 2020, EndYear: 1991}),
		Entry("both span forms", power.FetchRequest{
			Parameters: []string{"RH2M"}, Community: "AG", StartYear: 1991, EndYear: 2020,
			StartDate: "1981-01-01", EndDate: "1981-01-31"}),
		Entry("inverted date range", power.FetchRequest{
			Parameters: []string{"RH2M"}, Community: "AG",
			StartDate: "2020-02-01", EndDate: "2020-01-01"}),
		Entry("malformed start date", power.FetchRequest{
			Parameters: []string{"RH2M"}, Community: "AG",
			StartDate: "1981/01/01", EndDate: "2020-01-01"}),
	)
})

var _ = Describe("Client.FetchBatched", func() {
	It("passes an under-cap request through in a single call", func() {
		var calls atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			calls.Add(1)
			servePowerJSON(w, map[string]map[string]float64{
				"RH2M": {"199101": 70.0},
			}, -999.0)
		}))
		DeferCleanup(srv.Close)

		c := power.NewClient(srv.URL, nil)
		resp, err := c.FetchBatched(context.Background(),
			monthlyRequest("RH2M", "T2MDEW"), power.MaxParametersPerRequestMonthly)
		Expect(err).NotTo(HaveOccurred())
		Expect(calls.Load()).To(Equal(int32(1)), "2 params fit under the cap — no split")
		Expect(resp.Properties.Parameter).To(Equal(map[string]map[string]float64{
			"RH2M": {"199101": 70.0},
		}))
	})

	DescribeTable("splits an over-cap parameter list into caller-order chunks and merges",
		func(batchCap, wantBatches int) {
			all := power.DefaultParameters()
			Expect(len(all)).To(BeNumerically(">", batchCap),
				"the registry must exceed the cap for this spec to bite")
			idx := make(map[string]int, len(all))
			for i, name := range all {
				idx[name] = i
			}

			// Each sub-request's raw parameter string is recorded, and each
			// parameter's series value encodes its caller-order position —
			// so a chunking or merge slip surfaces on the offending
			// parameter, not as a bare count.
			var mu sync.Mutex
			var batches []string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				raw := r.URL.Query().Get("parameters")
				mu.Lock()
				batches = append(batches, raw)
				mu.Unlock()
				params := map[string]map[string]float64{}
				for _, p := range strings.Split(raw, ",") {
					params[p] = map[string]float64{"199101": float64(idx[p])}
				}
				servePowerJSON(w, params, -999.0)
			}))
			DeferCleanup(srv.Close)

			c := power.NewClient(srv.URL, nil)
			resp, err := c.FetchBatched(context.Background(), monthlyRequest(all...), batchCap)
			Expect(err).NotTo(HaveOccurred())

			var wantChunks []string
			for start := 0; start < len(all); start += batchCap {
				end := min(start+batchCap, len(all))
				wantChunks = append(wantChunks, strings.Join(all[start:end], ","))
			}
			Expect(batches).To(HaveLen(wantBatches))
			Expect(batches).To(Equal(wantChunks))

			want := map[string]map[string]float64{}
			for i, name := range all {
				want[name] = map[string]float64{"199101": float64(i)}
			}
			Expect(resp.Properties.Parameter).To(Equal(want))
			Expect(resp.Header.FillValue).To(Equal(-999.0))
		},
		Entry("monthly cap: 31 parameters split 25+6", power.MaxParametersPerRequestMonthly, 2),
		Entry("daily cap: 31 parameters split 20+11", power.MaxParametersPerRequestDaily, 2),
	)

	It("aborts when sub-responses disagree on fill_value", func() {
		// A fill sentinel that shifts between sub-requests would make the
		// merged response unable to tell real values from gaps — an
		// integrity failure, so the whole fetch must abort.
		var calls atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fill := -999.0
			if calls.Add(1) > 1 {
				fill = -888.0
			}
			params := map[string]map[string]float64{}
			for _, p := range strings.Split(r.URL.Query().Get("parameters"), ",") {
				params[p] = map[string]float64{"199101": 1.0}
			}
			servePowerJSON(w, params, fill)
		}))
		DeferCleanup(srv.Close)

		c := power.NewClient(srv.URL, nil)
		resp, err := c.FetchBatched(context.Background(),
			monthlyRequest(power.DefaultParameters()...), power.MaxParametersPerRequestMonthly)
		Expect(err).To(MatchError(ContainSubstring("fill_value")))
		Expect(resp).To(BeNil())
		Expect(calls.Load()).To(Equal(int32(2)), "the mismatch surfaces at the second sub-response")
	})

	It("rejects a non-positive batch cap", func() {
		c := power.NewClient("http://unused.invalid", nil)
		_, err := c.FetchBatched(context.Background(), monthlyRequest(), 0)
		Expect(err).To(HaveOccurred())
	})
})
