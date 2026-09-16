package ingest_test

import (
	"context"
	"database/sql"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/ingest"
)

// iPtr returns a pointer to v; a fresh allocation per call so specs can
// hand distinct counts to distinct cells.
func iPtr(v int) *int { return &v }

// fullPeriod builds 12 months of CellCounts with the same count per
// variable — the canonical uniform-station shape the boundary specs use.
func fullPeriod(tmax, tmin, precip int) []ingest.CellCounts {
	out := make([]ingest.CellCounts, 12)
	for i := range 12 {
		out[i] = ingest.CellCounts{
			Month:               i + 1,
			TmaxYearsWithData:   iPtr(tmax),
			TminYearsWithData:   iPtr(tmin),
			PrecipYearsWithData: iPtr(precip),
		}
	}
	return out
}

var _ = Describe("ScoreWMO", func() {
	It("returns hadAny=false for no cells, so the caller stores NULL not 0", func() {
		s, hadAny := ingest.ScoreWMO(nil)
		Expect(hadAny).To(BeFalse())
		Expect(s).To(Equal(ingest.WMOScore{}))
	})

	It("scores the perfect station at 1.0 on both systems", func() {
		s, hadAny := ingest.ScoreWMO(fullPeriod(30, 30, 30))
		Expect(hadAny).To(BeTrue())
		Expect(s.Binary).To(BeNumerically("~", 1.0, 1e-9))
		Expect(s.Continuous).To(BeNumerically("~", 1.0, 1e-9))
	})

	It("passes the binary test at exactly 24 years (the ≥ semantics)", func() {
		s, hadAny := ingest.ScoreWMO(fullPeriod(24, 24, 24))
		Expect(hadAny).To(BeTrue())
		Expect(s.Binary).To(BeNumerically("~", 1.0, 1e-9), "≥ 24 passes")
		Expect(s.Continuous).To(BeNumerically("~", 24.0/30.0, 1e-9))
	})

	It("fails the binary test at 23 years while continuous stays dense", func() {
		s, hadAny := ingest.ScoreWMO(fullPeriod(23, 23, 23))
		Expect(hadAny).To(BeTrue())
		Expect(s.Binary).To(BeNumerically("~", 0.0, 1e-9))
		Expect(s.Continuous).To(BeNumerically("~", 23.0/30.0, 1e-9))
	})

	It("clamps counts above 30 so continuous never exceeds 1.0", func() {
		s, hadAny := ingest.ScoreWMO(fullPeriod(35, 35, 35))
		Expect(hadAny).To(BeTrue())
		Expect(s.Binary).To(BeNumerically("~", 1.0, 1e-9))
		Expect(s.Continuous).To(BeNumerically("~", 1.0, 1e-9))
	})

	It("scores a whole missing variable as 0 over the fixed 36-cell denominator", func() {
		cells := make([]ingest.CellCounts, 12)
		for i := range 12 {
			cells[i] = ingest.CellCounts{
				Month:             i + 1,
				TmaxYearsWithData: iPtr(30),
				TminYearsWithData: iPtr(30),
				// Precip section entirely absent → nil.
			}
		}
		s, hadAny := ingest.ScoreWMO(cells)
		Expect(hadAny).To(BeTrue())
		Expect(s.Binary).To(BeNumerically("~", 24.0/36.0, 1e-9))
		Expect(s.Continuous).To(BeNumerically("~", 24.0/36.0, 1e-9))
	})

	It("keeps the denominator at 36 when months are missing entirely", func() {
		cells := []ingest.CellCounts{
			{Month: 1, TmaxYearsWithData: iPtr(30), TminYearsWithData: iPtr(30), PrecipYearsWithData: iPtr(30)},
			{Month: 2, TmaxYearsWithData: iPtr(30), TminYearsWithData: iPtr(30), PrecipYearsWithData: iPtr(30)},
			{Month: 3, TmaxYearsWithData: iPtr(30), TminYearsWithData: iPtr(30), PrecipYearsWithData: iPtr(30)},
		}
		s, hadAny := ingest.ScoreWMO(cells)
		Expect(hadAny).To(BeTrue())
		Expect(s.Binary).To(BeNumerically("~", 9.0/36.0, 1e-9))
		Expect(s.Continuous).To(BeNumerically("~", 9.0/36.0, 1e-9))
	})

	It("returns hadAny=false when rows exist but every count is NULL", func() {
		cells := make([]ingest.CellCounts, 12)
		for i := range 12 {
			cells[i] = ingest.CellCounts{Month: i + 1}
		}
		_, hadAny := ingest.ScoreWMO(cells)
		Expect(hadAny).To(BeFalse())
	})
})

var _ = Describe("LoadExtrasForScoring", func() {
	ctx := context.Background()

	It("round-trips the three core counts per period and never leaks other stations", func() {
		db := openDB(filepath.Join(GinkgoT().TempDir(), "bioclima.db"))
		tx := beginTx(db)

		sid, err := ingest.UpsertStation(ctx, tx, ingest.StationUpsert{
			Source: ingest.SourceConaguaConventional, ExternalID: "1001", Name: "t",
		})
		Expect(err).NotTo(HaveOccurred())

		// Two months for the target station — distinct counts per
		// variable so a column swap in the SELECT surfaces. Month 2's
		// tmin is NULL to pin NULL → nil pointer.
		_, err = tx.ExecContext(ctx, `INSERT INTO monthly_normals_extras
			(station_id, period, month, tmax_years_with_data, tmin_years_with_data, precip_years_with_data)
			VALUES (?, '1991-2020', 1, 28, 29, 30)`, sid)
		Expect(err).NotTo(HaveOccurred())
		_, err = tx.ExecContext(ctx, `INSERT INTO monthly_normals_extras
			(station_id, period, month, tmax_years_with_data, tmin_years_with_data, precip_years_with_data)
			VALUES (?, '1991-2020', 2, 25, NULL, 27)`, sid)
		Expect(err).NotTo(HaveOccurred())

		// A second station's row that must NOT appear in the result.
		otherID, err := ingest.UpsertStation(ctx, tx, ingest.StationUpsert{
			Source: ingest.SourceConaguaConventional, ExternalID: "9999", Name: "o",
		})
		Expect(err).NotTo(HaveOccurred())
		_, err = tx.ExecContext(ctx, `INSERT INTO monthly_normals_extras
			(station_id, period, month, tmax_years_with_data)
			VALUES (?, '1991-2020', 1, 30)`, otherID)
		Expect(err).NotTo(HaveOccurred())

		byPeriod, err := ingest.LoadExtrasForScoring(ctx, tx, sid)
		Expect(err).NotTo(HaveOccurred())
		Expect(byPeriod).To(HaveLen(1), "only the target station's periods may appear")

		cells := byPeriod["1991-2020"]
		Expect(cells).To(HaveLen(2))
		Expect(cells[0].Month).To(Equal(1))
		Expect(cells[0].TmaxYearsWithData).To(HaveValue(Equal(28)))
		Expect(cells[0].TminYearsWithData).To(HaveValue(Equal(29)))
		Expect(cells[0].PrecipYearsWithData).To(HaveValue(Equal(30)))
		Expect(cells[1].Month).To(Equal(2))
		Expect(cells[1].TmaxYearsWithData).To(HaveValue(Equal(25)))
		Expect(cells[1].TminYearsWithData).To(BeNil(), "NULL must load as a nil pointer")
		Expect(cells[1].PrecipYearsWithData).To(HaveValue(Equal(27)))
	})
})

var _ = Describe("StoreWMOCompleteness", func() {
	ctx := context.Background()

	newStation := func(tx *sql.Tx) int64 {
		GinkgoHelper()
		sid, err := ingest.UpsertStation(ctx, tx, ingest.StationUpsert{
			Source: ingest.SourceConaguaConventional, ExternalID: "1001", Name: "t",
		})
		Expect(err).NotTo(HaveOccurred())
		return sid
	}

	It("round-trips all four periods with distinct values per (period, system)", func() {
		db := openDB(filepath.Join(GinkgoT().TempDir(), "bioclima.db"))
		tx := beginTx(db)
		sid := newStation(tx)

		Expect(ingest.StoreWMOCompleteness(ctx, tx, sid, map[string]ingest.WMOScore{
			"1961-1990": {Binary: 0.61, Continuous: 0.62},
			"1971-2000": {Binary: 0.71, Continuous: 0.72},
			"1981-2010": {Binary: 0.81, Continuous: 0.82},
			"1991-2020": {Binary: 0.91, Continuous: 0.92},
		})).To(Succeed())
		Expect(tx.Commit()).To(Succeed())

		Expect(selectWMO(db, sid)).To(Equal(wmoRow{
			Bin61: validFloat(0.61), Bin71: validFloat(0.71),
			Bin81: validFloat(0.81), Bin91: validFloat(0.91),
			Cont61: validFloat(0.62), Cont71: validFloat(0.72),
			Cont81: validFloat(0.82), Cont91: validFloat(0.92),
		}))
	})

	It("resets absent periods to NULL, including on the 1961-1990 window", func() {
		db := openDB(filepath.Join(GinkgoT().TempDir(), "bioclima.db"))
		tx := beginTx(db)
		sid := newStation(tx)

		// First store: 1991-2020 only.
		Expect(ingest.StoreWMOCompleteness(ctx, tx, sid, map[string]ingest.WMOScore{
			"1991-2020": {Binary: 0.9, Continuous: 0.85},
		})).To(Succeed())
		Expect(tx.Commit()).To(Succeed())
		Expect(selectWMO(db, sid)).To(Equal(wmoRow{
			Bin91: validFloat(0.9), Cont91: validFloat(0.85),
		}))

		// Re-store with ONLY 1971-2000: the stale 1991-2020 score must
		// reset to NULL rather than linger.
		tx2 := beginTx(db)
		Expect(ingest.StoreWMOCompleteness(ctx, tx2, sid, map[string]ingest.WMOScore{
			"1971-2000": {Binary: 0.5, Continuous: 0.4},
		})).To(Succeed())
		Expect(tx2.Commit()).To(Succeed())
		Expect(selectWMO(db, sid)).To(Equal(wmoRow{
			Bin71: validFloat(0.5), Cont71: validFloat(0.4),
		}))

		// And a re-store with only 1961-1990 clears the rest — the new
		// window honors the same contract as the original three.
		tx3 := beginTx(db)
		Expect(ingest.StoreWMOCompleteness(ctx, tx3, sid, map[string]ingest.WMOScore{
			"1961-1990": {Binary: 0.5, Continuous: 0.4},
		})).To(Succeed())
		Expect(tx3.Commit()).To(Succeed())
		Expect(selectWMO(db, sid)).To(Equal(wmoRow{
			Bin61: validFloat(0.5), Cont61: validFloat(0.4),
		}))
	})

	// Station 16108 (SAN CRISTOBAL) in the national DB: extras rows
	// exist for a period but every core count (tmax/tmin/precip) is
	// NULL — only evap and rain_days carry data. The period must score
	// as NULL (no scorable core data), never as 0.
	It("stores NULL for the national all-core-NULL shape (station 16108 SAN CRISTOBAL)", func() {
		db := openDB(filepath.Join(GinkgoT().TempDir(), "bioclima.db"))
		tx := beginTx(db)
		sid := newStation(tx)

		for m := 1; m <= 12; m++ {
			_, err := tx.ExecContext(ctx, `INSERT INTO monthly_normals_extras
				(station_id, period, month, evap_years_with_data, rain_days, rain_days_years_with_data)
				VALUES (?, '1981-2010', ?, 25, 4.5, 26)`, sid, m)
			Expect(err).NotTo(HaveOccurred())
		}

		byPeriod, err := ingest.LoadExtrasForScoring(ctx, tx, sid)
		Expect(err).NotTo(HaveOccurred())
		Expect(byPeriod["1981-2010"]).To(HaveLen(12), "the extras rows are present")

		scores := map[string]ingest.WMOScore{}
		for period, cells := range byPeriod {
			if s, hadAny := ingest.ScoreWMO(cells); hadAny {
				scores[period] = s
			}
		}
		Expect(scores).To(BeEmpty(), "all-NULL core counts must not produce a score")

		Expect(ingest.StoreWMOCompleteness(ctx, tx, sid, scores)).To(Succeed())
		Expect(tx.Commit()).To(Succeed())
		Expect(selectWMO(db, sid)).To(Equal(wmoRow{}), "all eight columns stay NULL")
	})
})
