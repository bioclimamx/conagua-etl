package fetcher

import (
	"context"
	"encoding/json"
	"math/rand/v2"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// hardFloor is the interval floor implied by the compile-time politeness
// ceiling: no sampled delay may ever undercut it.
var hardFloor = time.Duration(float64(time.Second) / HardMaxRPS)

var _ = Describe("RateConfig", func() {
	Describe("Validate", func() {
		It("accepts the default config", func() {
			Expect(DefaultRateConfig().Validate()).To(Succeed())
		})

		DescribeTable("rejects invalid configs",
			func(mutate func(*RateConfig)) {
				cfg := DefaultRateConfig()
				mutate(&cfg)
				Expect(cfg.Validate()).To(HaveOccurred())
			},
			Entry("TargetRPS zero", func(c *RateConfig) { c.TargetRPS = 0 }),
			Entry("TargetRPS negative", func(c *RateConfig) { c.TargetRPS = -1 }),
			Entry("MaxRPS zero", func(c *RateConfig) { c.MaxRPS = 0 }),
			Entry("MaxRPS negative", func(c *RateConfig) { c.MaxRPS = -2 }),
			Entry("MaxRPS above the hard cap", func(c *RateConfig) { c.MaxRPS = HardMaxRPS + 0.1 }),
			Entry("TargetRPS above MaxRPS", func(c *RateConfig) { c.TargetRPS = c.MaxRPS + 1 }),
			Entry("JitterFraction negative", func(c *RateConfig) { c.JitterFraction = -0.1 }),
			Entry("MaxInterval zero", func(c *RateConfig) { c.MaxInterval = 0 }),
			Entry("MaxInterval negative", func(c *RateConfig) { c.MaxInterval = -time.Second }),
			Entry("MaxInterval below MinInterval", func(c *RateConfig) {
				c.TargetRPS = 4
				c.MaxRPS = 4
				c.MaxInterval = time.Millisecond
			}),
			Entry("RestEveryMin zero", func(c *RateConfig) { c.RestEveryMin = 0 }),
			Entry("RestEveryMax below RestEveryMin", func(c *RateConfig) {
				c.RestEveryMin = 10
				c.RestEveryMax = 5
			}),
			Entry("RestDurationMin negative", func(c *RateConfig) { c.RestDurationMin = -time.Second }),
			Entry("RestDurationMax below RestDurationMin", func(c *RateConfig) {
				c.RestDurationMin = 10 * time.Second
				c.RestDurationMax = 5 * time.Second
			}),
		)

		It("names the hard cap when MaxRPS exceeds it", func() {
			cfg := DefaultRateConfig()
			cfg.MaxRPS = HardMaxRPS + 1
			Expect(cfg.Validate()).To(MatchError(ContainSubstring("hard cap")))
		})

		It("names both bounds when MaxInterval undercuts MinInterval", func() {
			// A MaxInterval below the rate floor would let the cap clamp
			// sustained intervals under 1/HardMaxRPS — Validate must refuse.
			cfg := DefaultRateConfig()
			cfg.TargetRPS = 4
			cfg.MaxRPS = 4
			cfg.MaxInterval = time.Millisecond
			err := cfg.Validate()
			Expect(err).To(MatchError(ContainSubstring("MaxInterval")))
			Expect(err).To(MatchError(ContainSubstring(cfg.MaxInterval.String())))
			Expect(err).To(MatchError(ContainSubstring(cfg.MinInterval().String())))
		})
	})

	DescribeTable("MeanInterval is 1/TargetRPS",
		func(rps float64, want time.Duration) {
			cfg := DefaultRateConfig()
			cfg.TargetRPS = rps
			Expect(cfg.MeanInterval()).To(Equal(want))
		},
		Entry("1 req/s", 1.0, time.Second),
		Entry("2 req/s", 2.0, 500*time.Millisecond),
		Entry("0.5 req/s", 0.5, 2*time.Second),
		Entry("zero (guard)", 0.0, time.Duration(0)),
		Entry("negative (guard)", -1.0, time.Duration(0)),
	)

	DescribeTable("MinInterval honours both MaxRPS and the hard cap",
		func(maxRPS float64, want time.Duration) {
			cfg := DefaultRateConfig()
			cfg.MaxRPS = maxRPS
			Expect(cfg.MinInterval()).To(Equal(want))
		},
		Entry("within the cap", 4.0, 250*time.Millisecond),
		Entry("at the cap", 5.0, 200*time.Millisecond),
		Entry("zero falls back to the hard cap", 0.0, 200*time.Millisecond),
		Entry("negative falls back to the hard cap", -3.0, 200*time.Millisecond),
		Entry("above the cap falls back to the hard cap", 100.0, 200*time.Millisecond),
	)

	Describe("ledger serialization", func() {
		// The marshalled config is persisted into the pull ledger
		// (_progress.json); these keys are the audit/replay contract.
		It("marshals with the pinned ledger keys", func() {
			b, err := json.Marshal(DefaultRateConfig())
			Expect(err).NotTo(HaveOccurred())
			Expect(b).To(MatchJSON(`{
				"target_rps": 1,
				"max_rps": 4,
				"jitter_fraction": 0.3,
				"max_interval": 3000000000,
				"rest_every_min": 50,
				"rest_every_max": 200,
				"rest_duration_min": 30000000000,
				"rest_duration_max": 120000000000
			}`))
		})

		It("round-trips every field", func() {
			in := RateConfig{
				TargetRPS:       2.5,
				MaxRPS:          5,
				JitterFraction:  0.15,
				MaxInterval:     1500 * time.Millisecond,
				RestEveryMin:    7,
				RestEveryMax:    11,
				RestDurationMin: 250 * time.Millisecond,
				RestDurationMax: 900 * time.Millisecond,
			}
			b, err := json.Marshal(in)
			Expect(err).NotTo(HaveOccurred())
			var out RateConfig
			Expect(json.Unmarshal(b, &out)).To(Succeed())
			Expect(out).To(Equal(in))
		})
	})
})

var _ = Describe("Limiter", func() {
	Describe("NewLimiter", func() {
		It("rejects a config that fails Validate", func() {
			cfg := DefaultRateConfig()
			cfg.MaxRPS = HardMaxRPS + 1
			l, err := NewLimiter(cfg, 1)
			Expect(err).To(HaveOccurred())
			Expect(l).To(BeNil())
		})

		It("round-trips seed and config", func() {
			cfg := DefaultRateConfig()
			l, err := NewLimiter(cfg, 42)
			Expect(err).NotTo(HaveOccurred())
			Expect(l.Seed()).To(Equal(uint64(42)))
			Expect(l.Config()).To(Equal(cfg))
		})
	})

	Describe("determinism", func() {
		// Rest windows shrunk so rests occur inside the probed prefix; jitter
		// stays at the default 0.3 so the Normal sampler is exercised too.
		newCfg := func() RateConfig {
			cfg := DefaultRateConfig()
			cfg.RestEveryMin = 5
			cfg.RestEveryMax = 9
			cfg.RestDurationMin = 5 * time.Millisecond
			cfg.RestDurationMax = 10 * time.Millisecond
			return cfg
		}

		It("produces an identical wait/rest sequence for the same seed and config", func() {
			a, err := NewLimiter(newCfg(), 123)
			Expect(err).NotTo(HaveOccurred())
			b, err := NewLimiter(newCfg(), 123)
			Expect(err).NotTo(HaveOccurred())
			Expect(probeWaits(a, 120)).To(Equal(probeWaits(b, 120)))
		})

		It("diverges for a different seed", func() {
			a, err := NewLimiter(newCfg(), 123)
			Expect(err).NotTo(HaveOccurred())
			c, err := NewLimiter(newCfg(), 124)
			Expect(err).NotTo(HaveOccurred())
			Expect(probeWaits(a, 120)).NotTo(Equal(probeWaits(c, 120)))
		})
	})

	Describe("delay clamping", func() {
		It("clamps every sampled delay to [MinInterval, MaxInterval] under extreme jitter", func() {
			cfg := DefaultRateConfig()
			cfg.JitterFraction = 5.0 // σ = 5×mean: most raw samples fall outside the window
			l, err := NewLimiter(cfg, 42)
			Expect(err).NotTo(HaveOccurred())

			floorHits, capHits := 0, 0
			for _, info := range probeWaits(l, 500) {
				Expect(info.Delay).To(And(
					BeNumerically(">=", cfg.MinInterval()),
					BeNumerically("<=", cfg.MaxInterval),
				))
				if info.Delay == cfg.MinInterval() {
					floorHits++
				}
				if info.Delay == cfg.MaxInterval {
					capHits++
				}
			}
			// Both clamps must actually engage, or the spec proves nothing.
			Expect(floorHits).To(BeNumerically(">", 0))
			Expect(capHits).To(BeNumerically(">", 0))
		})
	})

	Describe("hard cap", func() {
		It("floors delays at 1/HardMaxRPS for a config running at the cap", func() {
			cfg := DefaultRateConfig()
			cfg.TargetRPS = HardMaxRPS
			cfg.MaxRPS = HardMaxRPS
			cfg.JitterFraction = 1.0
			l, err := NewLimiter(cfg, 1)
			Expect(err).NotTo(HaveOccurred())
			for _, info := range probeWaits(l, 300) {
				Expect(info.Delay).To(BeNumerically(">=", hardFloor))
			}
		})

		DescribeTable("backstops adversarial configs that bypassed Validate",
			// Constructed directly (package-internal), skipping NewLimiter:
			// proves the sampleDelay floor holds even for configs no valid
			// construction path can produce.
			func(mutate func(*RateConfig)) {
				cfg := DefaultRateConfig()
				cfg.JitterFraction = 1.0
				mutate(&cfg)
				l := &Limiter{cfg: cfg, rng: rand.New(rand.NewPCG(9, 9)), nextRestAt: 1 << 30}
				for _, info := range probeWaits(l, 300) {
					Expect(info.Delay).To(BeNumerically(">=", hardFloor))
				}
			},
			Entry("MaxRPS far above the cap", func(c *RateConfig) {
				c.TargetRPS, c.MaxRPS = 100, 100
			}),
			Entry("MaxRPS zero", func(c *RateConfig) {
				c.TargetRPS, c.MaxRPS = 50, 0
			}),
			Entry("MaxRPS negative", func(c *RateConfig) {
				c.TargetRPS, c.MaxRPS = 50, -3
			}),
			Entry("MaxInterval below the floor", func(c *RateConfig) {
				// Were the MaxInterval clamp applied after the floor, this
				// config would sustain ~1 ms intervals (~1000 req/s).
				c.TargetRPS, c.MaxRPS = 4, 4
				c.JitterFraction = 0.3
				c.MaxInterval = time.Millisecond
			}),
		)
	})

	Describe("rest cadence", func() {
		It("rests exactly on schedule when the cadence window is degenerate", func() {
			cfg := DefaultRateConfig()
			cfg.RestEveryMin, cfg.RestEveryMax = 5, 5
			cfg.RestDurationMin, cfg.RestDurationMax = 10*time.Millisecond, 10*time.Millisecond
			l, err := NewLimiter(cfg, 7)
			Expect(err).NotTo(HaveOccurred())

			seq := probeWaits(l, 26)
			for i, info := range seq {
				tick := i + 1
				// The first rest fires on the tick after RestEvery requests
				// have run, then every RestEvery ticks: 6, 11, 16, 21, 26.
				wantRest := tick >= 6 && (tick-6)%5 == 0
				Expect(info.RestFired).To(Equal(wantRest), "tick %d", tick)
				if wantRest {
					Expect(info.Rest).To(Equal(10*time.Millisecond), "tick %d", tick)
				} else {
					Expect(info.Rest).To(BeZero(), "tick %d", tick)
				}
				Expect(info.Total).To(Equal(info.Delay+info.Rest), "tick %d", tick)

				// Full counter round-trip: RequestsSinceRest counts up from 1
				// after each rest; UntilNextRest counts down to 0 on the tick
				// before the next rest.
				var wantSince int
				if tick < 6 {
					wantSince = tick
				} else {
					wantSince = (tick-6)%5 + 1
				}
				Expect(info.RequestsSinceRest).To(Equal(wantSince), "tick %d", tick)
				Expect(info.UntilNextRest).To(Equal(5-wantSince), "tick %d", tick)
			}
		})

		It("keeps cadence within [RestEveryMin, RestEveryMax] and durations within [RestDurationMin, RestDurationMax]", func() {
			cfg := DefaultRateConfig()
			cfg.RestEveryMin, cfg.RestEveryMax = 3, 6
			cfg.RestDurationMin, cfg.RestDurationMax = 5*time.Millisecond, 9*time.Millisecond
			l, err := NewLimiter(cfg, 99)
			Expect(err).NotTo(HaveOccurred())

			var restTicks, gaps []int
			for i, info := range probeWaits(l, 300) {
				tick := i + 1
				Expect(info.RestFired).To(Equal(info.Rest != 0), "tick %d", tick)
				if !info.RestFired {
					continue
				}
				Expect(info.Rest).To(And(
					BeNumerically(">=", cfg.RestDurationMin),
					BeNumerically("<=", cfg.RestDurationMax),
				), "tick %d", tick)
				if n := len(restTicks); n > 0 {
					gaps = append(gaps, tick-restTicks[n-1])
				}
				restTicks = append(restTicks, tick)
			}

			// The first rest fires after RestEvery ∈ [3, 6] requests, on the
			// following tick; every later gap is a fresh Uniform[3, 6] draw.
			Expect(restTicks).NotTo(BeEmpty())
			Expect(restTicks[0]).To(And(BeNumerically(">=", 4), BeNumerically("<=", 7)))
			Expect(gaps).NotTo(BeEmpty())
			for _, g := range gaps {
				Expect(g).To(And(BeNumerically(">=", 3), BeNumerically("<=", 6)))
			}
			// The cadence must actually vary — a constant gap would mean the
			// sampler degenerated.
			Expect(gaps).To(ContainElement(Not(Equal(gaps[0]))))
		})
	})

	Describe("Wait", func() {
		It("returns immediately with the context error when ctx is already cancelled", func() {
			l, err := NewLimiter(DefaultRateConfig(), 1)
			Expect(err).NotTo(HaveOccurred())
			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			start := time.Now()
			info, err := l.Wait(ctx)
			Expect(err).To(MatchError(context.Canceled))
			Expect(time.Since(start)).To(BeNumerically("<", 100*time.Millisecond))
			// The tick was still consumed: the WaitInfo describes it.
			Expect(info.Delay).To(BeNumerically(">", 0))
		})

		It("unblocks promptly when ctx cancels mid-wait", func() {
			cfg := DefaultRateConfig()
			cfg.TargetRPS = 0.2 // 5 s mean — the wait must NOT run to completion
			cfg.JitterFraction = 0
			cfg.MaxInterval = 10 * time.Second
			l, err := NewLimiter(cfg, 1)
			Expect(err).NotTo(HaveOccurred())

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			start := time.Now()
			_, err = l.Wait(ctx)
			Expect(err).To(MatchError(context.DeadlineExceeded))
			Expect(time.Since(start)).To(BeNumerically("<", 500*time.Millisecond))
		})
	})

	Describe("RandomSeed", func() {
		It("never returns the zero sentinel", func() {
			for range 200 {
				Expect(RandomSeed()).NotTo(BeZero())
			}
		})
	})
})
