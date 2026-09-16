package conagua

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"time"
)

// UserAgent is sent on every request to CONAGUA's SMN.
//
// TODO(bioclima): once bioclima.mx is live and SMN operators can reasonably
// be expected to have heard of it, switch this back to a descriptive,
// project-identifying string (e.g. "bioclima-etl/X.Y (+https://bioclima.mx)").
// For now we present as a regular browser so we don't get singled out by
// naive UA-based rate-limiting while the project is still unknown.
const UserAgent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

// DefaultTimeout is the per-request timeout; generous, since the SMN server
// can be slow under load.
const DefaultTimeout = 60 * time.Second

// Client fetches pages and files from CONAGUA's SMN endpoint.
//
// It disables TLS certificate verification because CONAGUA's SMN endpoint
// has a long-running broken certificate chain. Acceptable here: the data
// is public government data, the snapshot is re-hashed into our own
// storage downstream, and no credentials are ever sent.
type Client struct {
	HTTP *http.Client
}

// NewClient returns a Client with the TLS opt-out, identifying User-Agent,
// and default timeout configured.
func NewClient() *Client {
	return &Client{
		HTTP: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // see type comment
			},
			Timeout: DefaultTimeout,
		},
	}
}

// FetchCatalog downloads the state catalog page for state under base (the
// CONAGUA BaseURL in production; overridable so hermetic tests can point
// the whole tree at a fixture server) and parses it into a slice of Station
// records. Relative hrefs resolve against the page URL inside ParseCatalog,
// so the parsed station-file URLs follow base automatically.
func (c *Client) FetchCatalog(ctx context.Context, state StateCode, base string) ([]Station, error) {
	pageURL := state.CatalogURLUnder(base)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, pageURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", UserAgent)

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", pageURL, err)
	}
	defer resp.Body.Close() //nolint:errcheck // read-side close; no recovery possible

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: unexpected status %d", pageURL, resp.StatusCode)
	}
	return ParseCatalog(state, pageURL, resp.Body)
}

// FetchFile issues a GET for rawURL and returns the open response body, the
// HTTP status code, and any transport error. The caller MUST close body,
// including on non-200 statuses — returning the body instead of discarding
// it lets sinks stream the bytes through a sha256 hasher without an extra
// buffer, and preserves error messages that CONAGUA sometimes returns in
// the body of a 4xx.
func (c *Client) FetchFile(ctx context.Context, rawURL string) (body io.ReadCloser, status int, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("User-Agent", UserAgent)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("GET %s: %w", rawURL, err)
	}
	return resp.Body, resp.StatusCode, nil
}
