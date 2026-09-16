package fetcher

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

// HardMaxRPS is the upper bound on the effective request rate the limiter
// will ever produce, regardless of RateConfig.MaxRPS. It's enforced inside
// sampleDelay as a floor on the sampled interval (1 / HardMaxRPS) so a
// misconfigured flag can't drive us past what CONAGUA's SMN is comfortable
// serving.
const HardMaxRPS = 5.0

// RateConfig is rate-first: the operator says "run at ~N req/s" and the
// limiter jitters around that. Mean inter-request interval is 1/TargetRPS;
// jitter stddev is JitterFraction × mean.
//
// The JSON tags are load-bearing: the marshalled config is persisted into
// the pull ledger (_progress.json) so a run's pacing is auditable and
// replayable.
type RateConfig struct {
	// TargetRPS is the aimed-at request rate. The primary knob.
	TargetRPS float64 `json:"target_rps"`

	// MaxRPS is the ceiling on the effective rate — individual sampled
	// intervals are floored at 1/MaxRPS. Must satisfy
	// 0 < MaxRPS ≤ HardMaxRPS. Lower this to be extra-polite (e.g. 3 when
	// CONAGUA is struggling); you can't raise it past HardMaxRPS.
	MaxRPS float64 `json:"max_rps"`

	// JitterFraction is the stddev of the sampled inter-request interval
	// as a fraction of the mean. 0 disables jitter; 0.3 means "±30%" in
	// Normal-1σ terms.
	JitterFraction float64 `json:"jitter_fraction"`

	// MaxInterval caps unusually long sampled intervals — protects
	// against pathological Normal tails that could produce a 30 s wait.
	// Must be ≥ MinInterval() so the cap can never undercut the rate floor.
	MaxInterval time.Duration `json:"max_interval"`

	// Rest cadence: after each rest, the next rest fires once
	// Uniform[RestEveryMin, RestEveryMax] more requests have run.
	RestEveryMin int `json:"rest_every_min"`
	RestEveryMax int `json:"rest_every_max"`

	// Rest duration: Uniform[RestDurationMin, RestDurationMax].
	RestDurationMin time.Duration `json:"rest_duration_min"`
	RestDurationMax time.Duration `json:"rest_duration_max"`
}

// DefaultRateConfig returns the agreed defaults: ~1 req/s average, jitter
// ±30%, ceiling a hair under the hard cap, 30–120 s rest every 50–200
// requests.
func DefaultRateConfig() RateConfig {
	return RateConfig{
		TargetRPS:       1.0,
		MaxRPS:          4.0,
		JitterFraction:  0.3,
		MaxInterval:     3 * time.Second,
		RestEveryMin:    50,
		RestEveryMax:    200,
		RestDurationMin: 30 * time.Second,
		RestDurationMax: 120 * time.Second,
	}
}

// Validate sanity-checks the configuration. Construction (NewLimiter) goes
// through this, so no config can circumvent the hard cap; sampleDelay's
// MinInterval floor backstops it at run time.
func (c RateConfig) Validate() error {
	var errs []error
	if c.TargetRPS <= 0 {
		errs = append(errs, errors.New("TargetRPS must be > 0"))
	}
	if c.MaxRPS <= 0 {
		errs = append(errs, errors.New("MaxRPS must be > 0"))
	}
	if c.MaxRPS > HardMaxRPS {
		errs = append(errs, fmt.Errorf("MaxRPS (%v) exceeds hard cap %v req/s", c.MaxRPS, HardMaxRPS))
	}
	if c.TargetRPS > c.MaxRPS {
		errs = append(errs, fmt.Errorf("TargetRPS (%v) exceeds MaxRPS (%v)", c.TargetRPS, c.MaxRPS))
	}
	if c.JitterFraction < 0 {
		errs = append(errs, errors.New("JitterFraction must be >= 0"))
	}
	if c.MaxInterval <= 0 {
		errs = append(errs, errors.New("MaxInterval must be > 0"))
	} else if c.MaxInterval < c.MinInterval() {
		errs = append(errs, fmt.Errorf("MaxInterval (%s) is below the interval floor MinInterval (%s)", c.MaxInterval, c.MinInterval()))
	}
	if c.RestEveryMin < 1 || c.RestEveryMax < c.RestEveryMin {
		errs = append(errs, fmt.Errorf("RestEvery range invalid: [%d, %d]", c.RestEveryMin, c.RestEveryMax))
	}
	if c.RestDurationMin < 0 || c.RestDurationMax < c.RestDurationMin {
		errs = append(errs, fmt.Errorf("RestDuration range invalid: [%s, %s]", c.RestDurationMin, c.RestDurationMax))
	}
	return errors.Join(errs...)
}

// MeanInterval returns 1/TargetRPS as a time.Duration.
func (c RateConfig) MeanInterval() time.Duration {
	if c.TargetRPS <= 0 {
		return 0
	}
	return time.Duration(float64(time.Second) / c.TargetRPS)
}

// MinInterval returns the floor on the sampled inter-request interval, taking
// both MaxRPS and HardMaxRPS into account.
func (c RateConfig) MinInterval() time.Duration {
	rps := c.MaxRPS
	if rps <= 0 || rps > HardMaxRPS {
		rps = HardMaxRPS
	}
	return time.Duration(float64(time.Second) / rps)
}

// WaitInfo describes the wait a single limiter tick produced. It carries
// the rest-cadence telemetry a renderer needs to print rest notices.
type WaitInfo struct {
	Delay     time.Duration // sampled inter-request delay (always applied)
	Rest      time.Duration // sampled rest duration; 0 when no rest fired
	RestFired bool          // true when this Wait included a rest
	Total     time.Duration // Delay + Rest

	// RequestsSinceRest is the post-tick count of requests since the last
	// rest fired (including this one). Resets to 1 on the tick that fires
	// a rest.
	RequestsSinceRest int

	// UntilNextRest is how many more requests will pass before the next
	// rest fires. 0 means "the next tick will rest".
	UntilNextRest int
}

// Limiter samples inter-request delays from a truncated Normal and inserts
// longer "rest" periods on a random cadence — the pacing half of presenting
// as a polite, human-shaped client to CONAGUA's SMN.
//
// Internal state is mutex-guarded so NextWait/Wait are safe from any
// goroutine, though the Fetcher drives the limiter from its single
// sequential dispatch goroutine — the mutex is a cheap consistency guard,
// never a contention point.
type Limiter struct {
	mu         sync.Mutex
	cfg        RateConfig
	rng        *rand.Rand
	seed       uint64
	count      int // requests served since the last rest
	nextRestAt int // count value that triggers the next rest
}

// NewLimiter builds a Limiter seeded with seed and the given cfg. Same seed
// + same config ⇒ identical wait sequence, so a national pull's pacing is
// reproducible and auditable.
func NewLimiter(cfg RateConfig, seed uint64) (*Limiter, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	l := &Limiter{
		cfg:  cfg,
		seed: seed,
		// PCG requires two uint64 seeds. Deriving the second from the
		// first keeps the operator-supplied seed a single integer.
		rng: rand.New(rand.NewPCG(seed, seed^0x9E3779B97F4A7C15)),
	}
	l.nextRestAt = l.sampleRestEvery()
	return l, nil
}

// RandomSeed returns a random nonzero seed for NewLimiter. Zero is excluded
// because it is the "unset, generate one" sentinel at the caller (the
// ledger's persisted seed field treats 0 as absent). The caller logs and
// persists the returned seed so the run can be replayed.
func RandomSeed() uint64 {
	for {
		if s := rand.Uint64(); s != 0 {
			return s
		}
	}
}

// Seed returns the seed this limiter was built with.
func (l *Limiter) Seed() uint64 { return l.seed }

// Config returns a snapshot of the RateConfig the limiter runs with.
func (l *Limiter) Config() RateConfig {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.cfg
}

// NextWait advances limiter state and returns the wait that should be
// observed *before* the next request. Does not sleep — Wait wraps it with
// a context-honouring timer.
func (l *Limiter) NextWait() WaitInfo {
	l.mu.Lock()
	defer l.mu.Unlock()

	var info WaitInfo
	if l.count >= l.nextRestAt {
		info.Rest = l.sampleRestDuration()
		info.RestFired = true
		l.count = 0
		l.nextRestAt = l.sampleRestEvery()
	}
	info.Delay = l.sampleDelay()
	info.Total = info.Rest + info.Delay
	l.count++
	info.RequestsSinceRest = l.count
	info.UntilNextRest = max(0, l.nextRestAt-l.count)
	return info
}

// Wait blocks for the sampled inter-request delay (and any rest period that
// fires), then returns the WaitInfo describing what it waited. Returns
// early with ctx.Err() when ctx cancels — cancellation is the only way to
// interrupt a wait.
func (l *Limiter) Wait(ctx context.Context) (WaitInfo, error) {
	info := l.NextWait()
	if info.Total <= 0 {
		return info, ctx.Err()
	}
	t := time.NewTimer(info.Total)
	defer t.Stop()
	select {
	case <-t.C:
		return info, nil
	case <-ctx.Done():
		return info, ctx.Err()
	}
}

// --- samplers ---------------------------------------------------------------

func (l *Limiter) sampleDelay() time.Duration {
	mean := l.cfg.MeanInterval()
	sigma := time.Duration(float64(mean) * l.cfg.JitterFraction)
	s := l.rng.NormFloat64()
	d := mean + time.Duration(s*float64(sigma))

	if d > l.cfg.MaxInterval {
		d = l.cfg.MaxInterval
	}
	// The floor (MinInterval, never below 1/HardMaxRPS) is applied last so
	// no other clamp can undercut the hard politeness ceiling — the runtime
	// backstop holds even for a Limiter holding a config Validate never saw.
	if floor := l.cfg.MinInterval(); d < floor {
		d = floor
	}
	return d
}

func (l *Limiter) sampleRestEvery() int {
	lo := l.cfg.RestEveryMin
	hi := l.cfg.RestEveryMax
	if hi <= lo {
		return lo
	}
	return lo + l.rng.IntN(hi-lo+1)
}

func (l *Limiter) sampleRestDuration() time.Duration {
	lo := l.cfg.RestDurationMin
	hi := l.cfg.RestDurationMax
	if hi <= lo {
		return lo
	}
	return lo + time.Duration(l.rng.Int64N(int64(hi-lo+1)))
}
