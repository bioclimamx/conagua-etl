package validate_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/validate"
)

// The bbox characterization fixture: the
// four severity branches (impossible → error; outside MX → warn; NULL
// coordinate → warn; in-bbox → silent) plus the (0, 0) sentinel, with
// the issue texts pinned as fixed strings.
var _ = Describe("bbox", func() {
	It("is silent on a clean DB and counts every station scanned", func() {
		db, _ := openTempDB()
		seedClean(db)
		res := runRule(db, "bbox")
		Expect(res.Findings).To(BeEmpty())
		Expect(res.Scanned).To(Equal(2))
	})

	It("reproduces the reference four-branch case", func() {
		db, _ := openTempDB()
		// Inside MX bbox (no warning).
		station(db, "ok", "Mérida", f64(20.98), f64(-89.65))
		// Null lat and lon (warn).
		nullID := station(db, "null", "no-coords", nil, nil)
		// Outside MX bbox but valid coords (warn).
		outsideID := station(db, "outside", "London", f64(51.5), f64(-0.1))
		// (0, 0) sentinel — error.
		zeroID := station(db, "zerozero", "wherever", f64(0.0), f64(0.0))
		// Lat > 90 — error.
		brokenID := station(db, "impossible", "broken", f64(99.0), f64(-100.0))

		res := runRule(db, "bbox")
		Expect(res.Scanned).To(Equal(5))
		Expect(res.Findings).To(HaveLen(4))
		Expect(severities(res.Findings)).To(Equal([]string{
			validate.SeverityWarn, validate.SeverityWarn, validate.SeverityError, validate.SeverityError,
		}))
		Expect(issues(res.Findings)).To(Equal([]string{
			`station conagua_conventional/null ("no-coords") has NULL lat and lon`,
			`station conagua_conventional/outside ("London") at lat=51.5000 lon=-0.1000 outside MX bbox [14.5,32.8]×[-118.5,-86.7] — likely typo`,
			`station conagua_conventional/zerozero ("wherever") at impossible lat=0.0000 lon=0.0000 — error blocks publish`,
			`station conagua_conventional/impossible ("broken") at impossible lat=99.0000 lon=-100.0000 — error blocks publish`,
		}))
		for i, want := range []int64{nullID, outsideID, zeroID, brokenID} {
			Expect(res.Findings[i].StationID).To(HaveValue(Equal(want)))
			Expect(res.Findings[i].RuleID).To(Equal("bbox"))
		}
		// The "Mérida" station should not appear at all.
		for _, issue := range issues(res.Findings) {
			Expect(issue).NotTo(ContainSubstring("Mérida"))
		}
	})

	It("names the one NULL coordinate when only one is missing", func() {
		db, _ := openTempDB()
		station(db, "nolat", "A", nil, f64(-99.1))
		station(db, "nolon", "B", f64(19.4), nil)
		res := runRule(db, "bbox")
		Expect(issues(res.Findings)).To(Equal([]string{
			`station conagua_conventional/nolat ("A") has NULL lat`,
			`station conagua_conventional/nolon ("B") has NULL lon`,
		}))
		Expect(severities(res.Findings)).To(Equal([]string{validate.SeverityWarn, validate.SeverityWarn}))
	})

	It("treats the envelope's edges as inside and each impossible bound as an error", func() {
		db, _ := openTempDB()
		station(db, "corner", "Isla Guadalupe", f64(29.0), f64(-118.4))
		station(db, "edge", "NE corner", f64(32.8), f64(-86.7))
		station(db, "lon-low", "west", f64(20.0), f64(-180.5))
		station(db, "lat-low", "south", f64(-90.5), f64(-99.0))
		res := runRule(db, "bbox")
		Expect(res.Scanned).To(Equal(4))
		Expect(issues(res.Findings)).To(Equal([]string{
			`station conagua_conventional/lon-low ("west") at impossible lat=20.0000 lon=-180.5000 — error blocks publish`,
			`station conagua_conventional/lat-low ("south") at impossible lat=-90.5000 lon=-99.0000 — error blocks publish`,
		}))
	})
})
