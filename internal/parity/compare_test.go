package parity_test

import (
	"context"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/conagua"
	"github.com/bioclimamx/conagua-etl/internal/parity"
)

// valueComparedKinds pins which kinds must be compared at parsed-value
// level: exactly the kinds with a parser. monthly and extremes have no
// parser in either repo and stay presence-only.
var valueComparedKinds = map[conagua.Kind]bool{
	conagua.KindDaily:            true,
	conagua.KindNormals1961_1990: true,
	conagua.KindNormals1971_2000: true,
	conagua.KindNormals1981_2010: true,
	conagua.KindNormals1991_2020: true,
}

// expectOtherKindsEmpty asserts that every kind except kind reports
// zero activity, so findings can never leak across kinds unnoticed.
func expectOtherKindsEmpty(report parity.Report, kind conagua.Kind) {
	GinkgoHelper()
	for _, kr := range report.Kinds {
		if kr.Kind == kind {
			continue
		}
		Expect(kr).To(Equal(parity.KindReport{
			Kind:          kr.Kind,
			ValueCompared: valueComparedKinds[kr.Kind],
		}), "kind %s must be empty", kr.Kind)
	}
}

var _ = Describe("CompareSnapshots", func() {
	Context("daily kind", func() {
		It("reports identical files as fully identical, ignoring the EMISIÓN stamp", func(ctx SpecContext) {
			body := dailyBody(obsName, threeRows()...)
			report := runCompare(ctx,
				map[string]string{"daily/01001": body},
				// A fresh emission stamp is CONAGUA's routine re-emission
				// noise; the parsers exclude it by construction, so the comparison must see no drift.
				map[string]string{"daily/01001": mustReplace(body, "17/04/2026", "01/06/2026")},
			)
			Expect(kindReportFor(report, conagua.KindDaily)).To(Equal(parity.KindReport{
				Kind:              conagua.KindDaily,
				ValueCompared:     true,
				BaseFiles:         1,
				NewFiles:          1,
				StationsBoth:      1,
				StationsIdentical: 1,
				RowsIdentical:     3,
			}))
			expectOtherKindsEmpty(report, conagua.KindDaily)
		})

		It("classifies new-only dates as appended rows", func(ctx SpecContext) {
			rows := threeRows()[:2]
			report := runCompare(ctx,
				map[string]string{"daily/01001": dailyBody(obsName, rows...)},
				map[string]string{"daily/01001": dailyBody(obsName, append(rows, "1985-01-04\t0\tNULO\t19\t8")...)},
			)
			Expect(kindReportFor(report, conagua.KindDaily)).To(Equal(parity.KindReport{
				Kind:            conagua.KindDaily,
				ValueCompared:   true,
				BaseFiles:       1,
				NewFiles:        1,
				StationsBoth:    1,
				StationsRevised: 1,
				RowsIdentical:   2,
				RowsAppended: parity.Capped[parity.RowRef]{Total: 1, Examples: []parity.RowRef{
					{Station: "01001", Date: "1985-01-04"},
				}},
			}))
			expectOtherKindsEmpty(report, conagua.KindDaily)
		})

		It("classifies base-only dates as removed rows", func(ctx SpecContext) {
			rows := threeRows()
			report := runCompare(ctx,
				map[string]string{"daily/01001": dailyBody(obsName, rows...)},
				map[string]string{"daily/01001": dailyBody(obsName, rows[0], rows[2])},
			)
			Expect(kindReportFor(report, conagua.KindDaily)).To(Equal(parity.KindReport{
				Kind:            conagua.KindDaily,
				ValueCompared:   true,
				BaseFiles:       1,
				NewFiles:        1,
				StationsBoth:    1,
				StationsRevised: 1,
				RowsIdentical:   2,
				RowsRemoved: parity.Capped[parity.RowRef]{Total: 1, Examples: []parity.RowRef{
					{Station: "01001", Date: "1985-01-02"},
				}},
			}))
			expectOtherKindsEmpty(report, conagua.KindDaily)
		})

		It("records revised values with station, date, field, base, and new", func(ctx SpecContext) {
			newRows := threeRows()
			newRows[1] = "1985-01-02\t2\t4.1\t22.4\t10.5"   // Precip 1.5→2, Tmax 21→22.4
			newRows[2] = "1985-01-03\tNULO\tNULO\t18\tNULO" // Tmax NULO→18
			report := runCompare(ctx,
				map[string]string{
					"daily/01001": dailyBody(obsName, threeRows()...),
					"daily/20105": dailyBody(obsName, threeRows()...),
				},
				map[string]string{
					"daily/01001": dailyBody(obsName, newRows...),
					"daily/20105": dailyBody(obsName, threeRows()...),
				},
			)
			Expect(kindReportFor(report, conagua.KindDaily)).To(Equal(parity.KindReport{
				Kind:              conagua.KindDaily,
				ValueCompared:     true,
				BaseFiles:         2,
				NewFiles:          2,
				StationsBoth:      2,
				StationsIdentical: 1, // 20105 untouched
				StationsRevised:   1,
				RowsIdentical:     4, // 01001's first row + 20105's three
				RowsRevised:       2,
				Revisions: parity.Capped[parity.Revision]{Total: 3, Examples: []parity.Revision{
					// Field order within a row follows the collector
					// (Tmax, Tmin, Precip, Evap); rows in date order.
					{Station: "01001", Date: "1985-01-02", Field: "Tmax", Base: "21", New: "22.4"},
					{Station: "01001", Date: "1985-01-02", Field: "Precip", Base: "1.5", New: "2"},
					{Station: "01001", Date: "1985-01-03", Field: "Tmax", Base: "NULL", New: "18"},
				}},
			}))
			expectOtherKindsEmpty(report, conagua.KindDaily)
		})

		It("surfaces station-header drift as a header diff", func(ctx SpecContext) {
			report := runCompare(ctx,
				map[string]string{"daily/01001": dailyBody(obsName, threeRows()...)},
				map[string]string{"daily/01001": dailyBody("EL LLANO", threeRows()...)},
			)
			Expect(kindReportFor(report, conagua.KindDaily)).To(Equal(parity.KindReport{
				Kind:            conagua.KindDaily,
				ValueCompared:   true,
				BaseFiles:       1,
				NewFiles:        1,
				StationsBoth:    1,
				StationsRevised: 1,
				RowsIdentical:   3,
				HeaderDiffs: parity.Capped[parity.Revision]{Total: 1, Examples: []parity.Revision{
					{Station: "01001", Field: "Header.Name", Base: obsName, New: "EL LLANO"},
				}},
			}))
			expectOtherKindsEmpty(report, conagua.KindDaily)
		})

		It("surfaces a warning-count change without inventing value diffs", func(ctx SpecContext) {
			rows := []string{
				"1985-01-01\t0\tNULO\t20\t10",
				"1985-01-02\t0\tNULO\t20\t10",
			}
			newRows := []string{
				rows[0],
				// GARBAGE parses to nil-with-warning; NULO to nil without.
				// Values stay equal, so only the warning profile drifts.
				"1985-01-02\t0\tGARBAGE\t20\t10",
			}
			report := runCompare(ctx,
				map[string]string{"daily/01001": dailyBody(obsName, rows...)},
				map[string]string{"daily/01001": dailyBody(obsName, newRows...)},
			)
			Expect(kindReportFor(report, conagua.KindDaily)).To(Equal(parity.KindReport{
				Kind:            conagua.KindDaily,
				ValueCompared:   true,
				BaseFiles:       1,
				NewFiles:        1,
				StationsBoth:    1,
				StationsRevised: 1,
				RowsIdentical:   2,
				WarningDiffs: parity.Capped[parity.WarningCountDiff]{Total: 1, Examples: []parity.WarningCountDiff{
					{Station: "01001", Base: 0, New: 1},
				}},
			}))
			expectOtherKindsEmpty(report, conagua.KindDaily)
		})
	})

	Context("snapshot layout", func() {
		It("keeps the last occurrence of a duplicated date, mirroring ingest's UPSERT", func(ctx SpecContext) {
			report := runCompare(ctx,
				map[string]string{"daily/01001": dailyBody(obsName,
					"1985-01-01\t0\tNULO\t20\t10",
					"1985-01-01\t0\tNULO\t21\t10",
				)},
				map[string]string{"daily/01001": dailyBody(obsName,
					"1985-01-01\t0\tNULO\t21\t10",
				)},
			)
			// If the first occurrence won, a Tmax 20→21 revision would
			// appear here instead of a fully-identical station.
			Expect(kindReportFor(report, conagua.KindDaily)).To(Equal(parity.KindReport{
				Kind:              conagua.KindDaily,
				ValueCompared:     true,
				BaseFiles:         1,
				NewFiles:          1,
				StationsBoth:      1,
				StationsIdentical: 1,
				RowsIdentical:     1,
			}))
		})

		It("ignores subdirs, staging leftovers, and non-txt entries in a kind dir", func(ctx SpecContext) {
			baseDir := GinkgoT().TempDir()
			newDir := GinkgoT().TempDir()
			body := dailyBody(obsName, threeRows()...)
			writeSnapshot(baseDir, map[string]string{"daily/01001": body})
			writeSnapshot(newDir, map[string]string{"daily/01001": body})

			kindDir := filepath.Join(baseDir, "daily")
			Expect(os.MkdirAll(filepath.Join(kindDir, "nested"), 0o755)).To(Succeed())
			Expect(os.WriteFile(filepath.Join(kindDir, ".tmp-01002.txt"), []byte("partial"), 0o644)).To(Succeed())
			Expect(os.WriteFile(filepath.Join(kindDir, "manifest.json"), []byte("{}"), 0o644)).To(Succeed())

			report, err := parity.CompareSnapshots(ctx, baseDir, newDir)
			Expect(err).NotTo(HaveOccurred())
			// Strays surface neither as stations (BaseOnly) nor as parse
			// errors — only the real station file is compared.
			Expect(kindReportFor(report, conagua.KindDaily)).To(Equal(parity.KindReport{
				Kind:              conagua.KindDaily,
				ValueCompared:     true,
				BaseFiles:         1,
				NewFiles:          1,
				StationsBoth:      1,
				StationsIdentical: 1,
				RowsIdentical:     3,
			}))
		})
	})

	Context("normals kinds", func() {
		It("reports identical files as identical, ignoring emission and availability stamps", func(ctx SpecContext) {
			body := normalsFixture()
			updated := mustReplace(body, "17/04/2026", "01/06/2026")
			updated = mustReplace(updated, "APRIL DE 2026", "JUNE DE 2026")
			report := runCompare(ctx,
				map[string]string{"normals_1991_2020/01001": body},
				map[string]string{"normals_1991_2020/01001": updated},
			)
			Expect(kindReportFor(report, conagua.KindNormals1991_2020)).To(Equal(parity.KindReport{
				Kind:              conagua.KindNormals1991_2020,
				ValueCompared:     true,
				BaseFiles:         1,
				NewFiles:          1,
				StationsBoth:      1,
				StationsIdentical: 1,
			}))
			expectOtherKindsEmpty(report, conagua.KindNormals1991_2020)
		})

		It("records a changed monthly NORMAL value with station, month, field, base, and new", func(ctx SpecContext) {
			body := normalsFixture()
			report := runCompare(ctx,
				map[string]string{"normals_1991_2020/01001": body},
				// February Tmax NORMAL 25.5 → 26.1.
				map[string]string{"normals_1991_2020/01001": mustReplace(body, "NORMAL\t\t\t23\t25.5", "NORMAL\t\t\t23\t26.1")},
			)
			Expect(kindReportFor(report, conagua.KindNormals1991_2020)).To(Equal(parity.KindReport{
				Kind:            conagua.KindNormals1991_2020,
				ValueCompared:   true,
				BaseFiles:       1,
				NewFiles:        1,
				StationsBoth:    1,
				StationsRevised: 1,
				Revisions: parity.Capped[parity.Revision]{Total: 1, Examples: []parity.Revision{
					{Station: "01001", Month: 2, Field: "Tmax", Base: "25.5", New: "26.1"},
				}},
			}))
			expectOtherKindsEmpty(report, conagua.KindNormals1991_2020)
		})

		It("records a changed extras row value", func(ctx SpecContext) {
			body := normalsFixture()
			report := runCompare(ctx,
				map[string]string{"normals_1991_2020/01001": body},
				// January AÑO DE MÁXIMA (Tmax section) 2017 → 2019.
				map[string]string{"normals_1991_2020/01001": mustReplace(body, "AÑO DE MÁXIMA\t\t2017\t", "AÑO DE MÁXIMA\t\t2019\t")},
			)
			Expect(kindReportFor(report, conagua.KindNormals1991_2020)).To(Equal(parity.KindReport{
				Kind:            conagua.KindNormals1991_2020,
				ValueCompared:   true,
				BaseFiles:       1,
				NewFiles:        1,
				StationsBoth:    1,
				StationsRevised: 1,
				Revisions: parity.Capped[parity.Revision]{Total: 1, Examples: []parity.Revision{
					{Station: "01001", Month: 1, Field: "TmaxMonthlyExtremeYear", Base: "2017", New: "2019"},
				}},
			}))
			expectOtherKindsEmpty(report, conagua.KindNormals1991_2020)
		})

		It("surfaces a changed period stamp as a parse-error finding", func(ctx SpecContext) {
			body := normalsFixture()
			report := runCompare(ctx,
				map[string]string{"normals_1991_2020/01001": body},
				// A 1981-2010 stamp inside the 1991-2020 kind dir fails the
				// parser's kind↔period cross-check on the new side.
				map[string]string{"normals_1991_2020/01001": mustReplace(body, "NORMAL CLIMATOLÓGICA 1991-2020", "NORMAL CLIMATOLÓGICA 1981-2010")},
			)
			Expect(kindReportFor(report, conagua.KindNormals1991_2020)).To(Equal(parity.KindReport{
				Kind:          conagua.KindNormals1991_2020,
				ValueCompared: true,
				BaseFiles:     1,
				NewFiles:      1,
				StationsBoth:  1,
				ParseErrors: parity.Capped[parity.ParseError]{Total: 1, Examples: []parity.ParseError{
					{Station: "01001", Side: parity.SideNew, Err: `period mismatch: file says "1981-2010", expected "1991-2020"`},
				}},
			}))
			Expect(report.TotalParseErrors()).To(Equal(1))
			expectOtherKindsEmpty(report, conagua.KindNormals1991_2020)
		})
	})

	Context("station universe", func() {
		It("counts one-sided stations per kind without ever parsing them", func(ctx SpecContext) {
			base := make(map[string]string, len(conagua.AllKinds))
			updated := make(map[string]string, len(conagua.AllKinds))
			for _, kind := range conagua.AllKinds {
				// Garbage bodies: any parse attempt on a one-sided station
				// would surface as a ParseError and fail the zero-value
				// assertion below.
				base[string(kind)+"/11111"] = "not a station file\n"
				updated[string(kind)+"/22222"] = "different garbage\n"
			}
			report := runCompare(ctx, base, updated)

			expected := make([]parity.KindReport, 0, len(conagua.AllKinds))
			for _, kind := range conagua.AllKinds {
				expected = append(expected, parity.KindReport{
					Kind:          kind,
					ValueCompared: valueComparedKinds[kind],
					BaseFiles:     1,
					NewFiles:      1,
					BaseOnly:      parity.Capped[string]{Total: 1, Examples: []string{"11111"}},
					NewOnly:       parity.Capped[string]{Total: 1, Examples: []string{"22222"}},
				})
			}
			Expect(report.Kinds).To(Equal(expected))
		})
	})

	Context("presence-only kinds (monthly, extremes)", func() {
		It("counts overlapping stations for presence but never compares their values", func(ctx SpecContext) {
			report := runCompare(ctx,
				map[string]string{
					"monthly/01001":  "monthly garbage A\n",
					"extremes/01001": "extremes garbage A\n",
				},
				map[string]string{
					"monthly/01001":  "monthly garbage B\n",
					"extremes/01001": "extremes garbage B\n",
				},
			)

			expected := make([]parity.KindReport, 0, len(conagua.AllKinds))
			for _, kind := range conagua.AllKinds {
				kr := parity.KindReport{Kind: kind, ValueCompared: valueComparedKinds[kind]}
				if kind == conagua.KindMonthly || kind == conagua.KindExtremes {
					// Bodies differ wildly on the two sides, yet nothing
					// beyond presence is reported: no parse errors, no
					// station outcomes, no revisions.
					kr.BaseFiles, kr.NewFiles, kr.StationsBoth = 1, 1, 1
				}
				expected = append(expected, kr)
			}
			Expect(report.Kinds).To(Equal(expected))
		})
	})

	Context("unparseable files", func() {
		It("surfaces a parse failure as a finding naming the station and side", func(ctx SpecContext) {
			report := runCompare(ctx,
				map[string]string{"daily/01001": ""}, // empty file: no header at all
				map[string]string{"daily/01001": dailyBody(obsName, threeRows()...)},
			)
			Expect(kindReportFor(report, conagua.KindDaily)).To(Equal(parity.KindReport{
				Kind:          conagua.KindDaily,
				ValueCompared: true,
				BaseFiles:     1,
				NewFiles:      1,
				StationsBoth:  1,
				ParseErrors: parity.Capped[parity.ParseError]{Total: 1, Examples: []parity.ParseError{
					{Station: "01001", Side: parity.SideBase, Err: "parse header: no station header found (EOF at line 1)"},
				}},
			}))
			expectOtherKindsEmpty(report, conagua.KindDaily)
		})
	})

	Context("input validation", func() {
		It("rejects a missing snapshot dir", func(ctx SpecContext) {
			valid := GinkgoT().TempDir()
			_, err := parity.CompareSnapshots(ctx, filepath.Join(valid, "absent"), valid)
			Expect(err).To(MatchError(os.ErrNotExist))
			Expect(err).To(MatchError(ContainSubstring("snapshot dir")))
		})

		It("rejects a snapshot path that is not a directory", func(ctx SpecContext) {
			tmp := GinkgoT().TempDir()
			file := filepath.Join(tmp, "plain.txt")
			Expect(os.WriteFile(file, []byte("x"), 0o644)).To(Succeed())
			_, err := parity.CompareSnapshots(ctx, file, tmp)
			Expect(err).To(MatchError(ContainSubstring("not a directory")))
		})

		It("rejects two dirs holding no station files at all", func(ctx SpecContext) {
			_, err := parity.CompareSnapshots(ctx, GinkgoT().TempDir(), GinkgoT().TempDir())
			Expect(err).To(MatchError(ContainSubstring("no station files under")))
		})

		It("honors context cancellation", func() {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_, err := parity.CompareSnapshots(ctx, GinkgoT().TempDir(), GinkgoT().TempDir())
			Expect(err).To(MatchError(context.Canceled))
		})
	})
})
