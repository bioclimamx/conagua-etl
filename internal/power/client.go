package power

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/bioclimamx/conagua-etl/internal/fetcher"
)

// Default values for the POWER endpoints and the AG community (the
// unit conventions agroclimatology returns are what our W/m² solar
// conversion is calibrated against — see SolarMJpm2dToWm2).
const (
	DefaultEndpointMonthly = "https://power.larc.nasa.gov/api/temporal/monthly/point"
	DefaultEndpointDaily   = "https://power.larc.nasa.gov/api/temporal/daily/point"
	DefaultCommunity       = "AG"
)

// FetchRequest is one POWER point-time-series query — one cell. The
// span is expressed either as a year range (monthly mode, populates
// StartYear/EndYear) or a calendar date range (daily mode, populates
// StartDate/EndDate). Exactly one of the two forms must be set;
// Validate enforces the exclusion. buildURL switches between the
// `start=YYYY&end=YYYY` and `start=YYYYMMDD&end=YYYYMMDD` query
// shapes based on which form is populated.
type FetchRequest struct {
	Lat, Lon   float64
	Parameters []string
	Community  string

	// Monthly mode.
	StartYear int
	EndYear   int

	// Daily mode. Accepts either "YYYY-MM-DD" or "YYYYMMDD". buildURL
	// normalizes to POWER's expected "YYYYMMDD" wire form.
	StartDate string
	EndDate   string
}

// Validate catches obvious request misuse before we hit the network.
// POWER will return its own errors for bad lat/lon, but a fast-fail
// here makes orchestrator bugs visible without burning HTTP budget.
func (r FetchRequest) Validate() error {
	if len(r.Parameters) == 0 {
		return fmt.Errorf("FetchRequest: Parameters is empty")
	}
	if r.Community == "" {
		return fmt.Errorf("FetchRequest: Community is empty")
	}
	daily := r.StartDate != "" || r.EndDate != ""
	monthly := r.StartYear != 0 || r.EndYear != 0
	switch {
	case daily && monthly:
		return fmt.Errorf("FetchRequest: set either StartYear/EndYear (monthly) or StartDate/EndDate (daily), not both")
	case !daily && !monthly:
		return fmt.Errorf("FetchRequest: must set StartYear/EndYear (monthly) or StartDate/EndDate (daily)")
	case monthly:
		if r.StartYear <= 0 || r.EndYear < r.StartYear {
			return fmt.Errorf("FetchRequest: invalid year range %d-%d", r.StartYear, r.EndYear)
		}
	case daily:
		s, err := normalizeDailyDate(r.StartDate)
		if err != nil {
			return fmt.Errorf("FetchRequest: invalid StartDate %q: %w", r.StartDate, err)
		}
		e, err := normalizeDailyDate(r.EndDate)
		if err != nil {
			return fmt.Errorf("FetchRequest: invalid EndDate %q: %w", r.EndDate, err)
		}
		if e < s {
			return fmt.Errorf("FetchRequest: EndDate %q is before StartDate %q", r.EndDate, r.StartDate)
		}
	}
	return nil
}

// normalizeDailyDate accepts "YYYY-MM-DD" or "YYYYMMDD" and returns the
// 8-digit "YYYYMMDD" form POWER expects. Returns an error for any
// other input shape; calendar validity (e.g. Feb 30) is not checked —
// POWER rejects invalid dates server-side and we surface that.
func normalizeDailyDate(s string) (string, error) {
	switch len(s) {
	case 10:
		if s[4] != '-' || s[7] != '-' {
			return "", fmt.Errorf("expected YYYY-MM-DD")
		}
		out := s[:4] + s[5:7] + s[8:10]
		for _, ch := range out {
			if ch < '0' || ch > '9' {
				return "", fmt.Errorf("non-numeric character in date")
			}
		}
		return out, nil
	case 8:
		for _, ch := range s {
			if ch < '0' || ch > '9' {
				return "", fmt.Errorf("non-numeric character in date")
			}
		}
		return s, nil
	default:
		return "", fmt.Errorf("expected YYYY-MM-DD or YYYYMMDD, got %d chars", len(s))
	}
}

// Response is the subset of POWER's response we care about. The
// per-parameter map's keys are "YYYYMM" strings; POWER also
// encodes per-year annual means as "YYYY13" (e.g. "199213" is
// 1992's annual mean), which the rollup discards via its
// m∈[1,12] check. No "ANN" key today — verified live 2026-05-16.
type Response struct {
	Properties struct {
		Parameter map[string]map[string]float64 `json:"parameter"`
	} `json:"properties"`
	Header struct {
		FillValue float64 `json:"fill_value"`
		// TODO(power): the header's api.version is not parsed here and
		// power_runs has no column for it, so publish's NOTICE cannot
		// state the POWER version served on the access dates; the v0.2
		// path records it per run on the next augment.
	} `json:"header"`
	Messages []string `json:"messages"`
}

// Client fetches POWER responses with polite pacing and
// exponential-backoff retry. Safe for concurrent use.
//
// Pacing is the injected fetcher.Limiter — the project-wide component
// (jittered intervals under the compile-time HardMaxRPS ceiling).
// Every HTTP attempt takes a limiter token: each sub-request of a
// batched fetch and each retry after backoff, so no code path can
// outrun the configured rate. A nil Limiter disables pacing (tests).
//
// Transport is per-source and POWER-owned: TLS verification stays on
// (POWER's .gov chain is valid — contrast with the CONAGUA client),
// an honest Accept header, no UA masquerade.
type Client struct {
	Endpoint    string
	HTTP        *http.Client
	Limiter     *fetcher.Limiter
	MaxAttempts int           // total attempts including the first; >= 1
	BaseBackoff time.Duration // initial wait between retries; doubles per attempt
}

// NewClient is the conventional constructor. Pass limiter=nil to
// disable pacing (useful for tests against httptest.Server).
func NewClient(endpoint string, limiter *fetcher.Limiter) *Client {
	return &Client{
		Endpoint:    endpoint,
		HTTP:        &http.Client{Timeout: 60 * time.Second},
		Limiter:     limiter,
		MaxAttempts: 4,
		BaseBackoff: 2 * time.Second,
	}
}

// httpError is returned for non-2xx POWER responses. Carrying the
// status code lets isRetryable distinguish 429/5xx (retry) from
// 4xx (programmer error, fail fast).
type httpError struct {
	Status int
	Body   string
}

func (e *httpError) Error() string {
	return fmt.Sprintf("POWER status %d: %s", e.Status, e.Body)
}

// Fetch runs one request with retry. The retry loop sleeps
// BaseBackoff after the first failed attempt, then doubles. Context
// cancellation is honored at every wait point.
func (c *Client) Fetch(ctx context.Context, req FetchRequest) (*Response, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	u, err := buildURL(c.Endpoint, req)
	if err != nil {
		return nil, err
	}

	var lastErr error
	backoff := c.BaseBackoff
	for attempt := 1; attempt <= c.MaxAttempts; attempt++ {
		if c.Limiter != nil {
			if _, err := c.Limiter.Wait(ctx); err != nil {
				return nil, err
			}
		}
		resp, err := c.do(ctx, u)
		if err == nil {
			return resp, nil
		}
		lastErr = err
		if !isRetryable(ctx, err) {
			return nil, err
		}
		if attempt < c.MaxAttempts {
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			backoff *= 2
		}
	}
	return nil, fmt.Errorf("after %d attempts: %w", c.MaxAttempts, lastErr)
}

func (c *Client) do(ctx context.Context, u string) (*Response, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Accept", "application/json")

	resp, err := c.HTTP.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close() //nolint:errcheck // read-side close; no recovery possible

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, &httpError{Status: resp.StatusCode, Body: string(body)}
	}
	var out Response
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	return &out, nil
}

// isRetryable classifies a Fetch error as transient (retry-worthy) or
// terminal. HTTP 5xx and 429 retry; other 4xx are a stable server
// answer and fail fast. Network errors retry, with one load-bearing
// distinction (shared with the fetcher classifier): once the outer
// ctx is done, every error is terminal — operator cancellation is not
// a server answer — while a per-request timeout under a still-live
// outer ctx is a transport failure and must retry.
func isRetryable(ctx context.Context, err error) bool {
	if ctx.Err() != nil {
		return false
	}
	var he *httpError
	if errors.As(err, &he) {
		return he.Status == http.StatusTooManyRequests || he.Status >= 500
	}
	return err != nil
}

// buildURL renders a FetchRequest as the canonical POWER point
// time-series URL. The same function would be reused by any future
// publish step to render each station's `power_query.example_url`,
// so the formatting is deliberately fixed (decimal coordinates, no
// scientific notation, parameters comma-joined in caller order).
//
// Daily-mode requests (StartDate non-empty) render `start=YYYYMMDD`
// and `end=YYYYMMDD`; monthly-mode (StartYear non-zero) renders
// `start=YYYY` and `end=YYYY`. Validate has already enforced
// exactly-one form is populated.
func buildURL(endpoint string, req FetchRequest) (string, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("parse endpoint: %w", err)
	}
	q := u.Query()
	q.Set("parameters", strings.Join(req.Parameters, ","))
	q.Set("community", req.Community)
	q.Set("longitude", strconv.FormatFloat(req.Lon, 'f', -1, 64))
	q.Set("latitude", strconv.FormatFloat(req.Lat, 'f', -1, 64))
	if req.StartDate != "" {
		s, err := normalizeDailyDate(req.StartDate)
		if err != nil {
			return "", fmt.Errorf("buildURL: StartDate: %w", err)
		}
		e, err := normalizeDailyDate(req.EndDate)
		if err != nil {
			return "", fmt.Errorf("buildURL: EndDate: %w", err)
		}
		q.Set("start", s)
		q.Set("end", e)
	} else {
		q.Set("start", strconv.Itoa(req.StartYear))
		q.Set("end", strconv.Itoa(req.EndYear))
	}
	q.Set("format", "JSON")
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// BuildURL is the exported form of buildURL, available for any
// future publish step that wants to render an `example_url` field
// per station.
func BuildURL(endpoint string, req FetchRequest) (string, error) {
	return buildURL(endpoint, req)
}

// POWER's documented cap on the `parameters=` query string. Both
// values were verified empirically against the live endpoints on
// 2026-05-16:
//   - Monthly: a 31-parameter request was rejected with "A maximum of
//     25 parameters are can currently be requested in one submission."
//   - Daily: a 25-parameter request was rejected with "A maximum of 20
//     parameters are can currently be requested in one submission."
//
// The two endpoints carry different caps, so callers pass the
// appropriate constant to FetchBatched.
const (
	MaxParametersPerRequestMonthly = 25
	MaxParametersPerRequestDaily   = 20
)

// FetchBatched is Fetch's multi-parameter wrapper. If the requested
// parameter list exceeds batchCap, the request is split into
// ceil(N / batchCap) sub-requests with the parameter list chunked in
// the caller's order, and the responses are merged into a single
// Response. Sub-request errors abort the whole call.
//
// batchCap is supplied per-call so the monthly puller (25) and the
// daily puller (20) share one splitter without duplicating the
// chunking logic. A non-positive batchCap is rejected — callers must
// pass one of the MaxParametersPerRequest* constants.
//
// Merge semantics: parameter maps are concatenated (POWER guarantees
// disjoint parameter keys per request); Messages are concatenated;
// FillValue must match across sub-responses (POWER has always
// returned -999 for both monthly and daily point queries — we verify
// defensively and fail fast on mismatch since a divergent fill would
// silently corrupt downstream rollup or daily ingest).
func (c *Client) FetchBatched(ctx context.Context, req FetchRequest, batchCap int) (*Response, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	if batchCap <= 0 {
		return nil, fmt.Errorf("FetchBatched: cap must be positive, got %d", batchCap)
	}
	if len(req.Parameters) <= batchCap {
		return c.Fetch(ctx, req)
	}

	merged := &Response{}
	merged.Properties.Parameter = map[string]map[string]float64{}
	fillSet := false

	for start := 0; start < len(req.Parameters); start += batchCap {
		end := start + batchCap
		if end > len(req.Parameters) {
			end = len(req.Parameters)
		}
		sub := req
		sub.Parameters = req.Parameters[start:end]
		resp, err := c.Fetch(ctx, sub)
		if err != nil {
			return nil, fmt.Errorf("batch %d-%d: %w", start, end-1, err)
		}
		if !fillSet {
			merged.Header.FillValue = resp.Header.FillValue
			fillSet = true
		} else if resp.Header.FillValue != merged.Header.FillValue {
			return nil, fmt.Errorf("POWER returned inconsistent fill_value across batches: %v vs %v",
				merged.Header.FillValue, resp.Header.FillValue)
		}
		for k, v := range resp.Properties.Parameter {
			merged.Properties.Parameter[k] = v
		}
		merged.Messages = append(merged.Messages, resp.Messages...)
	}
	return merged, nil
}
