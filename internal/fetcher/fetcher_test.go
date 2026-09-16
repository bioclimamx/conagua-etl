package fetcher

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/conagua"
	"github.com/bioclimamx/conagua-etl/internal/snapshot"
)

// cancelAfterPut wraps a Sink and cancels a context the instant a Put
// completes — the tightest reproduction of "body stored, then outer-ctx
// cancellation observed", the never-discard-completed-work exception.
type cancelAfterPut struct {
	snapshot.Sink
	cancel context.CancelFunc
}

func (s cancelAfterPut) Put(ctx context.Context, addr snapshot.Address, r io.Reader) (snapshot.PutResult, error) {
	res, err := s.Sink.Put(ctx, addr, r)
	if err == nil {
		s.cancel()
	}
	return res, err
}

// cancelAfterExists wraps a Sink and cancels a context the instant an
// Exists probe answers true — a skip verdict computed just as the
// operator hits Ctrl-C.
type cancelAfterExists struct {
	snapshot.Sink
	cancel context.CancelFunc
}

func (s cancelAfterExists) Exists(ctx context.Context, addr snapshot.Address) (bool, error) {
	ok, err := s.Sink.Exists(ctx, addr)
	if err == nil && ok {
		s.cancel()
	}
	return ok, err
}

// putFailsCancelShaped wraps a Sink and fails every Put with an error
// whose chain contains context.Canceled — while the OUTER ctx stays
// live. It pins the casualty test to outer-ctx *state*: a genuine
// failure that merely wraps a cancellation sentinel must still reach a
// verdict, or the run would silently discard real errors (the same bug
// class the retry classifier's outer-context-first check guards).
type putFailsCancelShaped struct {
	snapshot.Sink
}

func (putFailsCancelShaped) Put(context.Context, snapshot.Address, io.Reader) (snapshot.PutResult, error) {
	return snapshot.PutResult{}, fmt.Errorf("simulated store failure: %w", context.Canceled)
}

var _ = Describe("Fetcher.Run", func() {
	var (
		root    string
		sink    *snapshot.LocalFS
		results []Result
	)

	// dailyBody is the payload every stub 200 serves; specs round-trip it
	// bytewise through the sink.
	dailyBody := []byte("ESTACIÓN: 1001\n01/01/1981 12.5 0.0 NULO\n")

	BeforeEach(func() {
		root = GinkgoT().TempDir()
		sink = snapshot.NewLocalFS(root)
		results = nil
	})

	// newFetcher assembles the production wiring — real conagua.Client, real
	// LocalFS sink — with short retry delays and no limiter (the pacing spec
	// installs a real Limiter explicitly).
	newFetcher := func() *Fetcher {
		return &Fetcher{
			Client: conagua.NewClient(),
			Sink:   sink,
			Date:   snapDate,
			Retry:  RetryPolicy{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: 2 * time.Millisecond},
			Hooks:  Hooks{OnResult: func(r Result) { results = append(results, r) }},
		}
	}

	startServer := func(h http.HandlerFunc) *httptest.Server {
		srv := httptest.NewServer(h)
		DeferCleanup(srv.Close)
		return srv
	}

	taskFor := func(srv *httptest.Server, id, path string) Task {
		return Task{Station: testStation(id), Kind: conagua.KindDaily, URL: srv.URL + path}
	}

	Context("at the conagua.Client seam", func() {
		It("stores a 200 body and reports status, attempts, bytes, and sha256", func() {
			var hits atomic.Int32
			var gotUA atomic.Value
			srv := startServer(func(w http.ResponseWriter, r *http.Request) {
				hits.Add(1)
				gotUA.Store(r.Header.Get("User-Agent"))
				_, _ = w.Write(dailyBody)
			})
			task := taskFor(srv, "01001", "/Diarios/dia01001.txt")

			Expect(newFetcher().Run(context.Background(), []Task{task})).To(Succeed())

			Expect(hits.Load()).To(Equal(int32(1)))
			// The politeness masquerade travels through the fetcher path.
			Expect(gotUA.Load()).To(Equal(conagua.UserAgent))

			Expect(results).To(HaveLen(1))
			res := results[0]
			Expect(res.Task).To(Equal(task))
			Expect(res.Outcome).To(Equal(OutcomeFetched))
			Expect(res.Status).To(Equal(http.StatusOK))
			Expect(res.Attempts).To(Equal(1))
			Expect(res.Bytes).To(Equal(int64(len(dailyBody))))
			Expect(res.SHA256).To(Equal(sha256Hex(dailyBody)))
			Expect(res.Err).NotTo(HaveOccurred())
			Expect(res.HTTPElapsed).To(BeNumerically(">", 0))
			Expect(res.Elapsed).To(BeNumerically(">=", res.HTTPElapsed))

			stored, err := os.ReadFile(sink.Path(task.Address(snapDate)))
			Expect(err).NotTo(HaveOccurred())
			Expect(stored).To(Equal(dailyBody))
		})

		It("classifies a 404 as not_found without retrying", func() {
			var hits atomic.Int32
			srv := startServer(func(w http.ResponseWriter, _ *http.Request) {
				hits.Add(1)
				w.WriteHeader(http.StatusNotFound)
				_, _ = io.WriteString(w, "<html>no existe</html>")
			})
			task := taskFor(srv, "01002", "/Diarios/dia01002.txt")

			Expect(newFetcher().Run(context.Background(), []Task{task})).To(Succeed())

			Expect(hits.Load()).To(Equal(int32(1)), "a 404 is a stable answer — no retry may fire")
			Expect(results).To(HaveLen(1))
			res := results[0]
			Expect(res.Outcome).To(Equal(OutcomeNotFound))
			Expect(res.Status).To(Equal(http.StatusNotFound))
			Expect(res.Attempts).To(Equal(1))
			Expect(res.Bytes).To(BeZero())
			Expect(res.SHA256).To(BeEmpty())
			Expect(res.Err).NotTo(HaveOccurred())

			exists, err := sink.Exists(context.Background(), task.Address(snapDate))
			Expect(err).NotTo(HaveOccurred())
			Expect(exists).To(BeFalse(), "nothing may land in the sink for a 404")
		})

		DescribeTable("retries a transient status and succeeds on the next attempt",
			func(transient int) {
				var hits atomic.Int32
				srv := startServer(func(w http.ResponseWriter, _ *http.Request) {
					if hits.Add(1) == 1 {
						w.WriteHeader(transient)
						_, _ = io.WriteString(w, "try later")
						return
					}
					_, _ = w.Write(dailyBody)
				})
				task := taskFor(srv, "01003", "/Diarios/dia01003.txt")

				Expect(newFetcher().Run(context.Background(), []Task{task})).To(Succeed())

				Expect(hits.Load()).To(Equal(int32(2)))
				Expect(results).To(HaveLen(1), "retries must still yield exactly one OnResult")
				res := results[0]
				Expect(res.Outcome).To(Equal(OutcomeFetched))
				Expect(res.Status).To(Equal(http.StatusOK))
				Expect(res.Attempts).To(Equal(2))
				Expect(res.Bytes).To(Equal(int64(len(dailyBody))))
				Expect(res.SHA256).To(Equal(sha256Hex(dailyBody)))
				Expect(res.Err).NotTo(HaveOccurred())

				stored, err := os.ReadFile(sink.Path(task.Address(snapDate)))
				Expect(err).NotTo(HaveOccurred())
				Expect(stored).To(Equal(dailyBody))
			},
			Entry("429 then 200", http.StatusTooManyRequests),
			Entry("500 then 200", http.StatusInternalServerError),
		)

		It("exhausts MaxAttempts against a persistent 500 and reports error", func() {
			var hits atomic.Int32
			srv := startServer(func(w http.ResponseWriter, _ *http.Request) {
				hits.Add(1)
				w.WriteHeader(http.StatusInternalServerError)
			})
			task := taskFor(srv, "01004", "/Diarios/dia01004.txt")

			Expect(newFetcher().Run(context.Background(), []Task{task})).To(Succeed())

			Expect(hits.Load()).To(Equal(int32(3)))
			Expect(results).To(HaveLen(1))
			res := results[0]
			Expect(res.Outcome).To(Equal(OutcomeError))
			Expect(res.Status).To(Equal(http.StatusInternalServerError))
			Expect(res.Attempts).To(Equal(3))
			Expect(res.Err).To(MatchError(ContainSubstring("unexpected status 500")))

			exists, err := sink.Exists(context.Background(), task.Address(snapDate))
			Expect(err).NotTo(HaveOccurred())
			Expect(exists).To(BeFalse())
		})

		It("treats a 403 as terminal on the first attempt", func() {
			var hits atomic.Int32
			srv := startServer(func(w http.ResponseWriter, _ *http.Request) {
				hits.Add(1)
				w.WriteHeader(http.StatusForbidden)
			})
			task := taskFor(srv, "01005", "/Diarios/dia01005.txt")

			Expect(newFetcher().Run(context.Background(), []Task{task})).To(Succeed())

			Expect(hits.Load()).To(Equal(int32(1)), "a 4xx is a stable answer — no retry may fire")
			Expect(results).To(HaveLen(1))
			res := results[0]
			Expect(res.Outcome).To(Equal(OutcomeError))
			Expect(res.Status).To(Equal(http.StatusForbidden))
			Expect(res.Attempts).To(Equal(1))
			Expect(res.Err).To(MatchError(ContainSubstring("unexpected status 403")))
		})

		It("retries transport errors until attempts run out", func() {
			srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
			deadURL := srv.URL
			srv.Close() // connection refused from here on
			task := Task{Station: testStation("01006"), Kind: conagua.KindDaily, URL: deadURL + "/Diarios/dia01006.txt"}

			Expect(newFetcher().Run(context.Background(), []Task{task})).To(Succeed())

			Expect(results).To(HaveLen(1))
			res := results[0]
			Expect(res.Outcome).To(Equal(OutcomeError))
			Expect(res.Status).To(BeZero(), "no response ever arrived")
			Expect(res.Attempts).To(Equal(3), "transport errors are transient — every attempt must fire")
			Expect(res.Err).To(HaveOccurred())
		})

		It("stops promptly on outer-ctx cancellation without touching later tasks", func() {
			var hits atomic.Int32
			srv := startServer(func(w http.ResponseWriter, _ *http.Request) {
				hits.Add(1)
				_, _ = w.Write(dailyBody)
			})
			tasks := []Task{
				taskFor(srv, "01007", "/Diarios/dia01007.txt"),
				taskFor(srv, "01008", "/Diarios/dia01008.txt"),
				taskFor(srv, "01009", "/Diarios/dia01009.txt"),
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			f := newFetcher()
			recordResult := f.Hooks.OnResult
			f.Hooks.OnResult = func(r Result) {
				recordResult(r)
				cancel() // operator shutdown right after the first task lands
			}

			start := time.Now()
			Expect(f.Run(ctx, tasks)).To(MatchError(context.Canceled))
			Expect(time.Since(start)).To(BeNumerically("<", time.Second))
			Expect(hits.Load()).To(Equal(int32(1)), "no request may fire after cancellation")

			// Only the task that reached a verdict before the cancel has a
			// Result; the rest produced no OnResult at all.
			Expect(results).To(HaveLen(1))
			res := results[0]
			Expect(res.Task).To(Equal(tasks[0]))
			Expect(res.Outcome).To(Equal(OutcomeFetched))
			Expect(res.Bytes).To(Equal(int64(len(dailyBody))))
			Expect(res.SHA256).To(Equal(sha256Hex(dailyBody)))
			Expect(res.Err).NotTo(HaveOccurred())
		})

		It("aborts a backoff sleep promptly on cancel — a casualty, no OnResult", func() {
			var hits atomic.Int32
			srv := startServer(func(w http.ResponseWriter, _ *http.Request) {
				hits.Add(1)
				w.WriteHeader(http.StatusInternalServerError)
			})
			task := taskFor(srv, "01010", "/Diarios/dia01010.txt")

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			f := newFetcher()
			f.Retry = RetryPolicy{MaxAttempts: 3, BaseDelay: time.Hour, MaxDelay: time.Hour}
			go func() {
				time.Sleep(30 * time.Millisecond)
				cancel()
			}()

			start := time.Now()
			Expect(f.Run(ctx, []Task{task})).To(MatchError(context.Canceled))
			Expect(time.Since(start)).To(BeNumerically("<", time.Second), "the 1 h backoff must not run")
			Expect(hits.Load()).To(Equal(int32(1)))
			Expect(results).To(BeEmpty(),
				"a cancellation casualty reaches no verdict — its ledger state stays pending for plain resumption")
		})

		It("keeps the fetched Result when the ctx cancels right after the body is stored", func() {
			srv := startServer(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(dailyBody) })
			tasks := []Task{
				taskFor(srv, "01011", "/Diarios/dia01011.txt"),
				taskFor(srv, "01012", "/Diarios/dia01012.txt"),
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			f := newFetcher()
			f.Sink = cancelAfterPut{Sink: sink, cancel: cancel}

			Expect(f.Run(ctx, tasks)).To(MatchError(context.Canceled))

			// Completed work is never discarded: the stored task keeps its
			// full fetched Result; the unstarted task reaches no verdict.
			Expect(results).To(HaveLen(1))
			res := results[0]
			Expect(res.Task).To(Equal(tasks[0]))
			Expect(res.Outcome).To(Equal(OutcomeFetched))
			Expect(res.Status).To(Equal(http.StatusOK))
			Expect(res.Attempts).To(Equal(1))
			Expect(res.Bytes).To(Equal(int64(len(dailyBody))))
			Expect(res.SHA256).To(Equal(sha256Hex(dailyBody)))
			Expect(res.Err).NotTo(HaveOccurred())

			stored, err := os.ReadFile(sink.Path(tasks[0].Address(snapDate)))
			Expect(err).NotTo(HaveOccurred())
			Expect(stored).To(Equal(dailyBody))
		})

		It("keeps a skip verdict computed as the ctx dies — success shapes always report", func() {
			srv := startServer(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(dailyBody) })
			tasks := []Task{
				taskFor(srv, "01011", "/Diarios/dia01011.txt"),
				taskFor(srv, "01012", "/Diarios/dia01012.txt"),
			}
			_, err := sink.Put(context.Background(), tasks[0].Address(snapDate), bytes.NewReader(dailyBody))
			Expect(err).NotTo(HaveOccurred())

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			f := newFetcher()
			f.Sink = cancelAfterExists{Sink: sink, cancel: cancel}

			Expect(f.Run(ctx, tasks)).To(MatchError(context.Canceled))
			Expect(results).To(HaveLen(1))
			Expect(results[0].Task).To(Equal(tasks[0]))
			Expect(results[0].Outcome).To(Equal(OutcomeSkipped))
		})
	})

	Context("at the snapshot.Sink seam", func() {
		It("lands the file atomically — final name only, no temp residue", func() {
			srv := startServer(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(dailyBody) })
			task := taskFor(srv, "01001", "/Diarios/dia01001.txt")

			Expect(newFetcher().Run(context.Background(), []Task{task})).To(Succeed())

			path := sink.Path(task.Address(snapDate))
			entries, err := os.ReadDir(filepath.Dir(path))
			Expect(err).NotTo(HaveOccurred())
			names := make([]string, len(entries))
			for i, e := range entries {
				names[i] = e.Name()
			}
			Expect(names).To(Equal([]string{"01001.txt"}))

			stored, err := os.ReadFile(path)
			Expect(err).NotTo(HaveOccurred())
			Expect(stored).To(Equal(dailyBody))
			Expect(results[0].SHA256).To(Equal(sha256Hex(dailyBody)))
		})

		It("short-circuits to skipped_existing without any HTTP request", func() {
			prior := []byte("prior snapshot content — must survive untouched\n")
			var hits atomic.Int32
			srv := startServer(func(w http.ResponseWriter, _ *http.Request) {
				hits.Add(1)
				_, _ = w.Write(dailyBody)
			})
			task := taskFor(srv, "01001", "/Diarios/dia01001.txt")
			addr := task.Address(snapDate)
			_, err := sink.Put(context.Background(), addr, bytes.NewReader(prior))
			Expect(err).NotTo(HaveOccurred())

			Expect(newFetcher().Run(context.Background(), []Task{task})).To(Succeed())

			Expect(hits.Load()).To(BeZero(), "the Exists short-circuit must skip the network entirely")
			Expect(results).To(HaveLen(1))
			res := results[0]
			Expect(res.Outcome).To(Equal(OutcomeSkipped))
			Expect(res.Status).To(BeZero())
			Expect(res.Attempts).To(BeZero())
			Expect(res.Bytes).To(BeZero())
			Expect(res.SHA256).To(BeEmpty())
			Expect(res.Err).NotTo(HaveOccurred())

			stored, err := os.ReadFile(sink.Path(addr))
			Expect(err).NotTo(HaveOccurred())
			Expect(stored).To(Equal(prior), "a stored snapshot file is immutable — never rewritten")
		})

		It("classifies a Put failure like an HTTP failure: retried, then error", func() {
			if os.Geteuid() == 0 {
				Skip("permission-based Put failure cannot be provoked as root")
			}
			var hits atomic.Int32
			srv := startServer(func(w http.ResponseWriter, _ *http.Request) {
				hits.Add(1)
				_, _ = w.Write(dailyBody)
			})
			task := taskFor(srv, "01001", "/Diarios/dia01001.txt")
			addr := task.Address(snapDate)

			// The station file's directory exists but is read-only: Exists
			// still answers false (file absent), FetchFile succeeds, and
			// Put's temp-file creation fails.
			dir := filepath.Dir(sink.Path(addr))
			Expect(os.MkdirAll(dir, 0o755)).To(Succeed())
			Expect(os.Chmod(dir, 0o555)).To(Succeed())
			DeferCleanup(func() { _ = os.Chmod(dir, 0o755) })

			Expect(newFetcher().Run(context.Background(), []Task{task})).To(Succeed())

			Expect(hits.Load()).To(Equal(int32(3)), "each Put failure re-fetches — sink errors ride the same retry loop")
			Expect(results).To(HaveLen(1))
			res := results[0]
			Expect(res.Outcome).To(Equal(OutcomeError))
			Expect(res.Attempts).To(Equal(3))
			Expect(res.Status).To(Equal(http.StatusOK), "the HTTP side succeeded; the sink failed")
			Expect(res.Err).To(MatchError(ContainSubstring("sink put")))
		})

		It("reports an error verdict whose chain wraps a cancellation while the outer ctx is live", func() {
			var hits atomic.Int32
			srv := startServer(func(w http.ResponseWriter, _ *http.Request) {
				hits.Add(1)
				_, _ = w.Write(dailyBody)
			})
			task := taskFor(srv, "01001", "/Diarios/dia01001.txt")

			f := newFetcher()
			f.Sink = putFailsCancelShaped{Sink: sink}

			// The casualty rule keys on outer-ctx state, never on error
			// identity: with a live ctx this cancellation-shaped failure
			// is an unknown-class error — retried, exhausted, and
			// REPORTED. An errors.Is(…, context.Canceled) test here would
			// silently discard a genuine failure with errors=0.
			Expect(f.Run(context.Background(), []Task{task})).To(Succeed())

			Expect(hits.Load()).To(Equal(int32(3)), "unknown-class Put errors ride the retry loop")
			Expect(results).To(HaveLen(1))
			res := results[0]
			Expect(res.Outcome).To(Equal(OutcomeError))
			Expect(res.Attempts).To(Equal(3))
			Expect(res.Status).To(Equal(http.StatusOK))
			Expect(res.Err).To(MatchError(context.Canceled), "the wrapped sentinel stays in the chain")
			Expect(res.Err).To(MatchError(ContainSubstring("sink put")))
		})

		It("fails the task without any HTTP request when Exists itself errors", func() {
			if os.Geteuid() == 0 {
				Skip("permission-based Exists failure cannot be provoked as root")
			}
			var hits atomic.Int32
			srv := startServer(func(w http.ResponseWriter, _ *http.Request) {
				hits.Add(1)
				_, _ = w.Write(dailyBody)
			})
			task := taskFor(srv, "01001", "/Diarios/dia01001.txt")

			// An unsearchable date directory makes the Exists stat fail with
			// EACCES — a real error, not a clean "not found".
			dateDir := filepath.Join(root, "conagua-raw", snapDate)
			Expect(os.MkdirAll(dateDir, 0o755)).To(Succeed())
			Expect(os.Chmod(dateDir, 0o000)).To(Succeed())
			DeferCleanup(func() { _ = os.Chmod(dateDir, 0o755) })

			Expect(newFetcher().Run(context.Background(), []Task{task})).To(Succeed())

			Expect(hits.Load()).To(BeZero(), "an unverifiable sink must not trigger a fetch")
			Expect(results).To(HaveLen(1))
			res := results[0]
			Expect(res.Outcome).To(Equal(OutcomeError))
			Expect(res.Attempts).To(BeZero())
			Expect(res.Err).To(MatchError(ContainSubstring("sink exists")))
		})
	})

	Context("hooks and run continuation", func() {
		// mixedHandler serves the three outcome shapes by path.
		mixedHandler := func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/ok":
				_, _ = w.Write(dailyBody)
			case "/missing":
				w.WriteHeader(http.StatusNotFound)
			default:
				w.WriteHeader(http.StatusForbidden)
			}
		}

		It("delivers exactly one OnResult per verdict-reaching task, in task order, across mixed outcomes", func() {
			srv := startServer(mixedHandler)
			tasks := []Task{
				taskFor(srv, "01001", "/ok"),
				taskFor(srv, "01002", "/missing"),
				taskFor(srv, "01003", "/forbidden"),
			}

			Expect(newFetcher().Run(context.Background(), tasks)).To(Succeed())

			Expect(results).To(HaveLen(3))
			for i := range tasks {
				Expect(results[i].Task).To(Equal(tasks[i]), "OnResult order must follow task order")
			}
			Expect(results[0].Outcome).To(Equal(OutcomeFetched))
			Expect(results[1].Outcome).To(Equal(OutcomeNotFound))
			Expect(results[2].Outcome).To(Equal(OutcomeError))
		})

		It("continues past a failing task — later tasks still complete", func() {
			srv := startServer(mixedHandler)
			tasks := []Task{
				taskFor(srv, "01001", "/ok"),
				taskFor(srv, "01002", "/forbidden"),
				taskFor(srv, "01003", "/ok"),
			}

			Expect(newFetcher().Run(context.Background(), tasks)).To(Succeed())

			Expect(results).To(HaveLen(3))
			Expect(results[1].Outcome).To(Equal(OutcomeError))
			Expect(results[2].Outcome).To(Equal(OutcomeFetched))

			// The post-failure task's file actually landed, intact.
			stored, err := os.ReadFile(sink.Path(tasks[2].Address(snapDate)))
			Expect(err).NotTo(HaveOccurred())
			Expect(stored).To(Equal(dailyBody))
			Expect(results[2].SHA256).To(Equal(sha256Hex(dailyBody)))
		})

		It("fires OnWait before every HTTP attempt, with rest cadence flagged", func() {
			// The cheapest legal pacing: the hard-cap floor is exactly 200 ms
			// per request, so this spec sleeps ~600 ms — the price of proving
			// the fetcher→Limiter.Wait integration for real.
			cfg := DefaultRateConfig()
			cfg.TargetRPS = HardMaxRPS
			cfg.MaxRPS = HardMaxRPS
			cfg.JitterFraction = 0
			cfg.MaxInterval = time.Second
			cfg.RestEveryMin, cfg.RestEveryMax = 2, 2
			cfg.RestDurationMin, cfg.RestDurationMax = time.Millisecond, time.Millisecond

			const seed = 5
			lim, err := NewLimiter(cfg, seed)
			Expect(err).NotTo(HaveOccurred())

			var mu sync.Mutex
			var events []string
			logEvent := func(e string) { mu.Lock(); events = append(events, e); mu.Unlock() }

			var hits atomic.Int32
			srv := startServer(func(w http.ResponseWriter, _ *http.Request) {
				logEvent("http")
				if hits.Add(1) == 2 { // task 2, attempt 1
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				_, _ = w.Write(dailyBody)
			})
			tasks := []Task{
				taskFor(srv, "01001", "/a"),
				taskFor(srv, "01002", "/b"),
			}

			var waits []WaitInfo
			f := newFetcher()
			f.Limiter = lim
			f.Hooks = Hooks{
				OnResult: func(r Result) { logEvent("result"); results = append(results, r) },
				OnWait:   func(w WaitInfo) { logEvent("wait"); waits = append(waits, w) },
			}

			Expect(f.Run(context.Background(), tasks)).To(Succeed())

			Expect(results).To(HaveLen(2))
			Expect(results[0].Outcome).To(Equal(OutcomeFetched))
			Expect(results[1].Outcome).To(Equal(OutcomeFetched))
			Expect(results[1].Attempts).To(Equal(2))

			// One wait per HTTP attempt, each strictly before its request.
			mu.Lock()
			got := append([]string(nil), events...)
			mu.Unlock()
			Expect(got).To(Equal([]string{
				"wait", "http", "result", // task 1: single attempt
				"wait", "http", "wait", "http", "result", // task 2: 500 then 200
			}))

			// The waits the fetcher observed are exactly the limiter's
			// deterministic sequence: a probe limiter with the same seed and
			// config reproduces them tick for tick.
			probe, err := NewLimiter(cfg, seed)
			Expect(err).NotTo(HaveOccurred())
			Expect(waits).To(Equal(probeWaits(probe, 3)))

			// Cadence 2 ⇒ the third tick rests; the first two do not.
			Expect(waits[0].RestFired).To(BeFalse())
			Expect(waits[1].RestFired).To(BeFalse())
			Expect(waits[2].RestFired).To(BeTrue())
			Expect(waits[2].Rest).To(Equal(time.Millisecond))
			for _, w := range waits {
				Expect(w.Delay).To(Equal(200 * time.Millisecond))
			}
		})

		It("is safe with zero-value Hooks and a live limiter", func() {
			cfg := DefaultRateConfig()
			cfg.TargetRPS = HardMaxRPS
			cfg.MaxRPS = HardMaxRPS
			cfg.JitterFraction = 0
			lim, err := NewLimiter(cfg, 1)
			Expect(err).NotTo(HaveOccurred())

			srv := startServer(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(dailyBody) })
			task := taskFor(srv, "01001", "/Diarios/dia01001.txt")

			f := newFetcher()
			f.Limiter = lim
			f.Hooks = Hooks{} // both callbacks nil — must not panic
			Expect(f.Run(context.Background(), []Task{task})).To(Succeed())

			stored, err := os.ReadFile(sink.Path(task.Address(snapDate)))
			Expect(err).NotTo(HaveOccurred())
			Expect(stored).To(Equal(dailyBody))
		})
	})
})

var _ = Describe("BuildTasks", func() {
	// Synthetic URLs keyed by kind string — distinct per (station, kind) so
	// the expected-task literals below pin the exact URL carried through.
	const base = "https://smn.example"
	fileFor := func(kind conagua.Kind, id string) conagua.FileEntry {
		return conagua.FileEntry{URL: base + "/" + string(kind) + "/" + id + ".txt"}
	}

	var s1, s2 conagua.Station
	BeforeEach(func() {
		s1 = testStation("01001")
		s1.Files = map[conagua.Kind]conagua.FileEntry{}
		for _, k := range conagua.AllKinds {
			s1.Files[k] = fileFor(k, "01001")
		}
		s2 = testStation("01002")
		s2.Files = map[conagua.Kind]conagua.FileEntry{
			conagua.KindDaily:            fileFor(conagua.KindDaily, "01002"),
			conagua.KindNormals1991_2020: fileFor(conagua.KindNormals1991_2020, "01002"),
		}
	})

	It("flattens stations × kinds in canonical order, carrying every field", func() {
		Expect(BuildTasks([]conagua.Station{s1, s2}, nil)).To(Equal([]Task{
			{Station: s1, Kind: conagua.KindDaily, URL: base + "/daily/01001.txt"},
			{Station: s1, Kind: conagua.KindMonthly, URL: base + "/monthly/01001.txt"},
			{Station: s1, Kind: conagua.KindExtremes, URL: base + "/extremes/01001.txt"},
			{Station: s1, Kind: conagua.KindNormals1961_1990, URL: base + "/normals_1961_1990/01001.txt"},
			{Station: s1, Kind: conagua.KindNormals1971_2000, URL: base + "/normals_1971_2000/01001.txt"},
			{Station: s1, Kind: conagua.KindNormals1981_2010, URL: base + "/normals_1981_2010/01001.txt"},
			{Station: s1, Kind: conagua.KindNormals1991_2020, URL: base + "/normals_1991_2020/01001.txt"},
			{Station: s2, Kind: conagua.KindDaily, URL: base + "/daily/01002.txt"},
			{Station: s2, Kind: conagua.KindNormals1991_2020, URL: base + "/normals_1991_2020/01002.txt"},
		}))
	})

	It("filters to the requested kinds, emitting in canonical (not filter) order", func() {
		Expect(BuildTasks(
			[]conagua.Station{s1, s2},
			[]conagua.Kind{conagua.KindExtremes, conagua.KindDaily}, // reversed on purpose
		)).To(Equal([]Task{
			{Station: s1, Kind: conagua.KindDaily, URL: base + "/daily/01001.txt"},
			{Station: s1, Kind: conagua.KindExtremes, URL: base + "/extremes/01001.txt"},
			{Station: s2, Kind: conagua.KindDaily, URL: base + "/daily/01002.txt"},
		}))
	})

	It("treats an empty filter slice like nil: include all kinds", func() {
		stations := []conagua.Station{s1, s2}
		Expect(BuildTasks(stations, []conagua.Kind{})).To(Equal(BuildTasks(stations, nil)))
	})

	It("returns an empty plan for no stations", func() {
		Expect(BuildTasks(nil, nil)).To(BeEmpty())
	})
})

var _ = Describe("Outcome", func() {
	// The fetcher→ledger translation is a plain cast (pull.go does
	// snapshot.FileOutcome(r.Outcome)), so the string values on both
	// sides of the seam must match verbatim.
	DescribeTable("casts to the matching snapshot.FileOutcome",
		func(o Outcome, want snapshot.FileOutcome) {
			Expect(snapshot.FileOutcome(o)).To(Equal(want))
			Expect(string(o)).To(Equal(string(want)))
		},
		Entry("fetched", OutcomeFetched, snapshot.OutcomeFetched),
		Entry("skipped_existing", OutcomeSkipped, snapshot.OutcomeSkipped),
		Entry("not_found", OutcomeNotFound, snapshot.OutcomeNotFound),
		Entry("error", OutcomeError, snapshot.OutcomeError),
	)
})

var _ = Describe("Task.Address", func() {
	It("derives the sink address from station, kind, and the given date", func() {
		task := Task{
			Station: testStation("01001"),
			Kind:    conagua.KindExtremes,
			URL:     "https://smn.example/Med-Extr/medex01001.txt",
		}
		Expect(task.Address("2026-06-08")).To(Equal(snapshot.Address{
			Date:      "2026-06-08",
			Kind:      conagua.KindExtremes,
			StationID: "01001",
		}))
	})
})
