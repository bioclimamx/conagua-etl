package parity_test

import (
	"encoding/json"
	"strconv"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/parity"
	"github.com/bioclimamx/conagua-etl/internal/power"
)

// gfloat mirrors the comparator's exact float rendering.
func gfloat(v float64) string { return strconv.FormatFloat(v, 'g', -1, 64) }

// findingFields extracts the Field of every retained manifest finding.
func findingFields(cmp *parity.PowerComparison) []string {
	fields := make([]string, 0, len(cmp.ManifestFindings.Samples))
	for _, f := range cmp.ManifestFindings.Samples {
		fields = append(fields, f.Field)
	}
	return fields
}

var _ = Describe("ComparePower cell math", func() {
	It("reproduces a faithful database with zero findings", func(ctx SpecContext) {
		db := seedFaithfulPowerDB()
		cmp := mustComparePower(ctx, db)

		Expect(cmp.StationsCompared).To(Equal(3))
		Expect(cmp.Cells.RowsCompared).To(Equal(2))
		Expect(cmp.Cells.RowsIdentical).To(Equal(2))
		Expect(cmp.Cells.Clean()).To(BeTrue())
		Expect(cmp.Links.RowsCompared).To(Equal(3))
		Expect(cmp.Links.RowsIdentical).To(Equal(3))
		Expect(cmp.Links.Clean()).To(BeTrue())
		Expect(cmp.ManifestFindings.Total).To(BeZero())
		Expect(cmp.Clean()).To(BeTrue())
	})

	It("round-trips every loaded manifest column", func(ctx SpecContext) {
		db := seedFaithfulPowerDB()
		cmp := mustComparePower(ctx, db)
		Expect(cmp.Runs).To(HaveLen(2))

		monthly := cmp.Runs[0]
		Expect(monthly.ID).To(Equal(int64(1)))
		Expect(monthly.Temporal).To(Equal(string(power.TemporalMonthly)))
		Expect(monthly.EndpointURL).To(Equal(power.DefaultEndpointMonthly))
		Expect(monthly.Parameters).To(Equal(strings.Join(power.DefaultParameters(), ",")))
		Expect(monthly.Community).To(Equal(power.DefaultCommunity))
		Expect(monthly.PeriodStartYear).To(Equal(int64(1991)))
		Expect(monthly.PeriodEndYear).To(Equal(int64(2020)))
		Expect(monthly.PeriodStartDate.Valid).To(BeFalse())
		Expect(monthly.PeriodEndDate.Valid).To(BeFalse())
		Expect(monthly.GridResolution).To(Equal(power.Resolution))
		Expect(monthly.SolarConversion).To(Equal(power.SolarMJpm2dToWm2))
		Expect(monthly.UnitConversions.Valid).To(BeTrue())
		var conv map[string]power.UnitConversion
		Expect(json.Unmarshal([]byte(monthly.UnitConversions.String), &conv)).To(Succeed())
		Expect(conv).To(Equal(power.Conversions))

		daily := cmp.Runs[1]
		Expect(daily.ID).To(Equal(int64(2)))
		Expect(daily.Temporal).To(Equal(string(power.TemporalDaily)))
		Expect(daily.EndpointURL).To(Equal(power.DefaultEndpointDaily))
		Expect(daily.PeriodStartYear).To(Equal(int64(1981)))
		Expect(daily.PeriodEndYear).To(Equal(int64(2026)))
		Expect(daily.PeriodStartDate.String).To(Equal("1981-01-01"))
		Expect(daily.PeriodEndDate.String).To(Equal("2026-06-08"))
	})

	It("flags a tampered stored cell centroid float-exactly", func(ctx SpecContext) {
		db := seedFaithfulPowerDB()
		shared := powerCellShared()
		execSQL(db, `UPDATE nasa_power_grid_cells SET lat = ? WHERE cell_id = ?`,
			shared.Lat+0.25, shared.ID)

		cmp := mustComparePower(ctx, db)
		Expect(cmp.Cells.RowsCompared).To(Equal(2))
		Expect(cmp.Cells.RowsIdentical).To(Equal(1))
		Expect(cmp.Cells.ValueDiffs.Total).To(Equal(1))
		Expect(cmp.Cells.ValueDiffs.Samples[0]).To(Equal(parity.PowerValueDiff{
			Key:     parity.DBKey{Row: shared.ID},
			Column:  "lat",
			Derived: gfloat(shared.Lat),
			Stored:  gfloat(shared.Lat + 0.25),
		}))
		Expect(cmp.Clean()).To(BeFalse())
	})

	It("flags a stored-only cell", func(ctx SpecContext) {
		db := seedFaithfulPowerDB()
		execSQL(db, `INSERT INTO nasa_power_grid_cells (cell_id, lat, lon, grid_resolution)
			VALUES ('10.0N_100.0000W', 10, -100, ?)`, power.Resolution)

		cmp := mustComparePower(ctx, db)
		Expect(cmp.Cells.StoredOnly.Total).To(Equal(1))
		Expect(cmp.Cells.StoredOnly.Samples[0]).To(Equal(parity.DBKey{Row: "10.0N_100.0000W"}))
		Expect(cmp.Cells.RowsCompared).To(Equal(2))
	})

	It("flags a derived-only cell", func(ctx SpecContext) {
		db := seedFaithfulPowerDB()
		solo := powerCellSolo()
		execSQL(db, `DELETE FROM nasa_power_grid_cells WHERE cell_id = ?`, solo.ID)

		cmp := mustComparePower(ctx, db)
		Expect(cmp.Cells.DerivedOnly.Total).To(Equal(1))
		Expect(cmp.Cells.DerivedOnly.Samples[0]).To(Equal(parity.DBKey{Row: solo.ID}))
		Expect(cmp.Cells.RowsCompared).To(Equal(1))
	})

	It("flags a tampered link distance float-exactly", func(ctx SpecContext) {
		db := seedFaithfulPowerDB()
		solo := powerCellSolo()
		execSQL(db, `UPDATE station_power_cell SET distance_km = 99.5 WHERE station_id = 13`)

		cmp := mustComparePower(ctx, db)
		Expect(cmp.Links.RowsCompared).To(Equal(3))
		Expect(cmp.Links.RowsIdentical).To(Equal(2))
		Expect(cmp.Links.ValueDiffs.Total).To(Equal(1))
		Expect(cmp.Links.ValueDiffs.Samples[0]).To(Equal(parity.PowerValueDiff{
			Key:     parity.DBKey{Source: seedSource, ExternalID: "2003"},
			Column:  "distance_km",
			Derived: gfloat(power.HaversineKm(25.10, -100.90, solo.Lat, solo.Lon)),
			Stored:  "99.5",
		}))
	})

	It("flags a tampered link cell assignment", func(ctx SpecContext) {
		db := seedFaithfulPowerDB()
		shared, solo := powerCellShared(), powerCellSolo()
		execSQL(db, `UPDATE station_power_cell SET cell_id = ? WHERE station_id = 13`, shared.ID)

		cmp := mustComparePower(ctx, db)
		Expect(cmp.Links.ValueDiffs.Total).To(Equal(1))
		Expect(cmp.Links.ValueDiffs.Samples[0]).To(Equal(parity.PowerValueDiff{
			Key:     parity.DBKey{Source: seedSource, ExternalID: "2003"},
			Column:  "cell_id",
			Derived: solo.ID,
			Stored:  shared.ID,
		}))
	})

	It("flags a station missing its link row", func(ctx SpecContext) {
		db := seedFaithfulPowerDB()
		execSQL(db, `DELETE FROM station_power_cell WHERE station_id = 12`)

		cmp := mustComparePower(ctx, db)
		Expect(cmp.Links.DerivedOnly.Total).To(Equal(1))
		Expect(cmp.Links.DerivedOnly.Samples[0]).To(Equal(
			parity.DBKey{Source: seedSource, ExternalID: "2002"}))
		Expect(cmp.Links.RowsCompared).To(Equal(2))
	})

	It("flags stored links outside the derived station universe", func(ctx SpecContext) {
		db := seedFaithfulPowerDB()
		shared := powerCellShared()
		// A station without coordinates never enters the cell math; a
		// dangling station_id cannot resolve to a natural key at all.
		seedPowerStationRow(db, 14, "2004", nil, nil)
		execSQL(db, `INSERT INTO station_power_cell (station_id, cell_id, distance_km)
			VALUES (14, ?, 1.0), (999, ?, 1.0)`, shared.ID, shared.ID)

		cmp := mustComparePower(ctx, db)
		Expect(cmp.Links.StoredOnly.Total).To(Equal(2))
		Expect(cmp.Links.StoredOnly.Samples).To(Equal([]parity.DBKey{
			{ExternalID: "unresolved-station-id:999"},
			{Source: seedSource, ExternalID: "2004"},
		}))
		Expect(cmp.Links.RowsCompared).To(Equal(3))
	})
})

var _ = Describe("ComparePower manifests", func() {
	It("checks only complete runs", func(ctx SpecContext) {
		db := seedFaithfulPowerDB()
		seedPowerRun(db, 3, power.TemporalMonthly, "running")
		seedPowerRun(db, 4, power.TemporalMonthly, "aborted")
		execSQL(db, `UPDATE power_runs SET endpoint_url = 'https://example.invalid' WHERE id IN (3, 4)`)

		cmp := mustComparePower(ctx, db)
		Expect(cmp.Runs).To(HaveLen(2))
		Expect(cmp.ManifestFindings.Total).To(BeZero())
	})

	It("flags a parameters string that deviates from the registry order", func(ctx SpecContext) {
		db := seedFaithfulPowerDB()
		full := strings.Join(power.DefaultParameters(), ",")
		tampered := strings.Join(power.DefaultParameters()[1:], ",")
		execSQL(db, `UPDATE power_runs SET parameters = ? WHERE id = 1`, tampered)

		cmp := mustComparePower(ctx, db)
		Expect(findingFields(cmp)).To(ConsistOf("parameters", "unit_conversions.T2M"))
		Expect(cmp.ManifestFindings.Samples).To(ContainElement(parity.ManifestFinding{
			RunID: 1, Field: "parameters", Got: tampered, Want: full,
		}))
		// The stored JSON still carries T2M, which the tampered run no
		// longer requests.
		Expect(cmp.ManifestFindings.Samples).To(ContainElement(parity.ManifestFinding{
			RunID: 1, Field: "unit_conversions.T2M",
			Got: "C → C ×1", Want: "absent (not in run parameters)",
		}))
	})

	It("flags a non-pinned endpoint", func(ctx SpecContext) {
		db := seedFaithfulPowerDB()
		execSQL(db, `UPDATE power_runs SET endpoint_url = 'https://example.com/power' WHERE id = 1`)

		cmp := mustComparePower(ctx, db)
		Expect(cmp.ManifestFindings.Samples).To(ConsistOf(parity.ManifestFinding{
			RunID: 1, Field: "endpoint_url",
			Got: "https://example.com/power", Want: power.DefaultEndpointMonthly,
		}))
	})

	It("flags a non-pinned community", func(ctx SpecContext) {
		db := seedFaithfulPowerDB()
		execSQL(db, `UPDATE power_runs SET community = 'RE' WHERE id = 2`)

		cmp := mustComparePower(ctx, db)
		Expect(cmp.ManifestFindings.Samples).To(ConsistOf(parity.ManifestFinding{
			RunID: 2, Field: "community", Got: "RE", Want: power.DefaultCommunity,
		}))
	})

	It("flags a tampered solar conversion factor", func(ctx SpecContext) {
		db := seedFaithfulPowerDB()
		execSQL(db, `UPDATE power_runs SET solar_conversion = 11.574 WHERE id = 1`)

		cmp := mustComparePower(ctx, db)
		Expect(cmp.ManifestFindings.Samples).To(ConsistOf(parity.ManifestFinding{
			RunID: 1, Field: "solar_conversion",
			Got: "11.574", Want: gfloat(power.SolarMJpm2dToWm2),
		}))
	})

	It("flags a semantically tampered unit_conversions factor", func(ctx SpecContext) {
		db := seedFaithfulPowerDB()
		conv := make(map[string]power.UnitConversion, len(power.Conversions))
		for k, v := range power.Conversions {
			conv[k] = v
		}
		conv["T2M"] = power.UnitConversion{PowerUnit: "C", StoredUnit: "C", Factor: 2}
		j, err := json.Marshal(conv)
		Expect(err).NotTo(HaveOccurred())
		execSQL(db, `UPDATE power_runs SET unit_conversions = ? WHERE id = 1`, string(j))

		cmp := mustComparePower(ctx, db)
		Expect(cmp.ManifestFindings.Samples).To(ConsistOf(parity.ManifestFinding{
			RunID: 1, Field: "unit_conversions.T2M",
			Got: "C → C ×2", Want: "C → C ×1",
		}))
	})

	It("flags a missing unit_conversions entry", func(ctx SpecContext) {
		db := seedFaithfulPowerDB()
		conv := make(map[string]power.UnitConversion, len(power.Conversions))
		for k, v := range power.Conversions {
			conv[k] = v
		}
		delete(conv, "RH2M")
		j, err := json.Marshal(conv)
		Expect(err).NotTo(HaveOccurred())
		execSQL(db, `UPDATE power_runs SET unit_conversions = ? WHERE id = 1`, string(j))

		cmp := mustComparePower(ctx, db)
		Expect(cmp.ManifestFindings.Samples).To(ConsistOf(parity.ManifestFinding{
			RunID: 1, Field: "unit_conversions.RH2M",
			Got: "absent", Want: "% → % ×1",
		}))
	})

	It("flags NULL unit_conversions", func(ctx SpecContext) {
		db := seedFaithfulPowerDB()
		execSQL(db, `UPDATE power_runs SET unit_conversions = NULL WHERE id = 1`)

		cmp := mustComparePower(ctx, db)
		Expect(cmp.ManifestFindings.Samples).To(ConsistOf(parity.ManifestFinding{
			RunID: 1, Field: "unit_conversions",
			Got: "NULL", Want: "per-parameter JSON manifest",
		}))
	})

	It("flags a daily run missing its date span", func(ctx SpecContext) {
		db := seedFaithfulPowerDB()
		execSQL(db, `UPDATE power_runs SET period_start_date = NULL WHERE id = 2`)

		cmp := mustComparePower(ctx, db)
		Expect(cmp.ManifestFindings.Samples).To(ConsistOf(parity.ManifestFinding{
			RunID: 2, Field: "url.span",
			Got:  "period_start_date=NULL period_end_date=2026-06-08",
			Want: "a daily manifest carries both dates",
		}))
	})
})
