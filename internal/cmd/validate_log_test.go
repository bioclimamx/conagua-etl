package cmd

// Direct specs for the validateLog renderer: the per-rule start and
// done lines, and the heartbeat that prints while a rule is in flight
// and stops — synchronously — when the rule finishes. The wall-clock
// fields (timestamp, elapsed) are matched structurally; everything the
// rule's result determines is pinned exactly.

import (
	"bytes"
	"regexp"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/validate"
)

// lockedBuffer is a bytes.Buffer safe for the heartbeat goroutine and
// the spec to share.
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

var stillRunningRE = regexp.MustCompile(`(?m)^\[\d{2}:\d{2}:\d{2}\] daily-sanity             still running · \d+(\.\d+)?m?s$`)

var _ = Describe("validateLog", func() {
	It("prints the start line with the rule's id padded to the column and its name", func() {
		var out bytes.Buffer
		r := &validateLog{err: &out, beat: newHeartbeat(&out, time.Hour)}
		r.ruleStart("bbox", "Lat/lon plausibility")
		r.finish()
		Expect(out.String()).To(MatchRegexp(`^\[\d{2}:\d{2}:\d{2}\] bbox                     starting \(Lat/lon plausibility\)\n$`))
	})

	It("prints the done line with the rows scanned, the severity tallies, and the elapsed time rounded to milliseconds", func() {
		var out bytes.Buffer
		r := &validateLog{err: &out, beat: newHeartbeat(&out, time.Hour)}
		id := int64(7)
		r.ruleDone("wmo-month-completeness", validate.RuleResult{
			Scanned: 927354,
			Findings: []validate.Finding{
				{RuleID: "wmo-month-completeness", Severity: validate.SeverityWarn, StationID: &id, Issue: "a"},
				{RuleID: "wmo-month-completeness", Severity: validate.SeverityWarn, StationID: &id, Issue: "b"},
				{RuleID: "wmo-month-completeness", Severity: validate.SeverityError, StationID: &id, Issue: "c"},
			},
		}, 33548*time.Millisecond+400*time.Microsecond)
		Expect(out.String()).To(MatchRegexp(
			`^\[\d{2}:\d{2}:\d{2}\] wmo-month-completeness   done · scanned=927354 warn=2 error=1 · 33\.548s\n$`))
	})

	It("beats while a rule runs and stops before the done line, never after it", func() {
		out := &lockedBuffer{}
		r := &validateLog{err: out, beat: newHeartbeat(out, 20*time.Millisecond)}
		r.ruleStart("daily-sanity", "Daily-series sanity")
		Eventually(func() int {
			return len(stillRunningRE.FindAllString(out.String(), -1))
		}, 2*time.Second, 5*time.Millisecond).Should(BeNumerically(">=", 2))
		r.ruleDone("daily-sanity", validate.RuleResult{Scanned: 3}, 60*time.Millisecond)

		after := out.String()
		Expect(after).To(HaveSuffix("done · scanned=3 warn=0 error=0 · 60ms\n"))
		Consistently(out.String, 100*time.Millisecond, 10*time.Millisecond).Should(Equal(after),
			"no heartbeat line after the done line")
		beats := stillRunningRE.FindAllString(after, -1)
		Expect(beats).NotTo(BeEmpty())
		Expect(strings.LastIndex(after, beats[len(beats)-1])).To(BeNumerically("<", strings.LastIndex(after, "done ·")))
	})

	It("finish stops a heartbeat a rule's error left running, and is safe with none running", func() {
		out := &lockedBuffer{}
		r := &validateLog{err: out, beat: newHeartbeat(out, 20*time.Millisecond)}
		r.finish()
		r.ruleStart("daily-sanity", "Daily-series sanity")
		Eventually(func() int {
			return len(stillRunningRE.FindAllString(out.String(), -1))
		}, 2*time.Second, 5*time.Millisecond).Should(BeNumerically(">=", 1))
		r.finish()
		after := out.String()
		Consistently(out.String, 100*time.Millisecond, 10*time.Millisecond).Should(Equal(after))
		r.finish()
	})

	It("restarts the heartbeat for the next rule, stopping the previous one first", func() {
		out := &lockedBuffer{}
		beat := newHeartbeat(out, 20*time.Millisecond)
		beat.start("daily-sanity")
		beat.start("cross-period")
		Eventually(func() string { return out.String() }, 2*time.Second, 5*time.Millisecond).
			Should(ContainSubstring("cross-period"))
		beat.stop()
		after := out.String()
		Consistently(out.String, 100*time.Millisecond, 10*time.Millisecond).Should(Equal(after))
		for _, line := range strings.Split(strings.TrimSpace(after), "\n") {
			Expect(line).To(MatchRegexp(`^\[\d{2}:\d{2}:\d{2}\] (daily-sanity|cross-period) +still running · \S+$`))
		}
	})
})

var _ = Describe("countSeverities", func() {
	It("tallies warn and error findings and ignores nothing else, since no third literal exists", func() {
		warns, errs := countSeverities([]validate.Finding{
			{Severity: validate.SeverityWarn}, {Severity: validate.SeverityError}, {Severity: validate.SeverityWarn},
		})
		Expect(warns).To(Equal(2))
		Expect(errs).To(Equal(1))
		warns, errs = countSeverities(nil)
		Expect(warns).To(BeZero())
		Expect(errs).To(BeZero())
	})
})
