package parity_test

import (
	"database/sql"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/parity"
	"github.com/bioclimamx/conagua-etl/internal/power"
)

var _ = Describe("PowerComparison.Markdown", func() {
	It("round-trips every findings category into its section table", func() {
		cmp := &parity.PowerComparison{
			Label:            "/db/ingest.db",
			StationsCompared: 3,
			Cells: parity.PowerTable{
				Table:         "nasa_power_grid_cells",
				RowsCompared:  2,
				RowsIdentical: 1,
				ValueDiffs: parity.Sampled[parity.PowerValueDiff]{Total: 1, Samples: []parity.PowerValueDiff{
					{Key: parity.DBKey{Row: "19.5N_99.3750W"}, Column: "lat", Derived: "19.5", Stored: "19.75"},
				}},
				DerivedOnly: parity.Sampled[parity.DBKey]{Total: 1, Samples: []parity.DBKey{
					{Row: "25.0N_100.6250W"},
				}},
				StoredOnly: parity.Sampled[parity.DBKey]{Total: 2, Samples: []parity.DBKey{
					{Row: "10.0N_100.0000W"},
				}},
			},
			Links: parity.PowerTable{Table: "station_power_cell", RowsCompared: 3, RowsIdentical: 3},
			Runs: []parity.PowerRunManifest{{
				ID:              7,
				Temporal:        "monthly",
				EndpointURL:     "https://power.example/api",
				Parameters:      "T2M,RH2M",
				Community:       "AG",
				PeriodStartYear: 1991,
				PeriodEndYear:   2020,
				GridResolution:  "0.5x0.625",
				SolarConversion: 11.5,
				UnitConversions: sql.NullString{
					String: `{"T2M":{"power_unit":"C","stored_unit":"C","factor":1},` +
						`"RH2M":{"power_unit":"%","stored_unit":"%","factor":1}}`,
					Valid: true,
				},
			}},
			ManifestFindings: parity.Sampled[parity.ManifestFinding]{Total: 1, Samples: []parity.ManifestFinding{
				{RunID: 7, Field: "parameters", Got: "T2M,RH2M", Want: "T2M,T2M_MAX"},
			}},
		}

		md := cmp.Markdown()

		Expect(md).To(ContainSubstring("# Power parity report"))
		Expect(md).To(ContainSubstring("- DB: `/db/ingest.db`"))
		Expect(md).To(ContainSubstring("Stations compared (source `conagua_conventional`, with coordinates): 3"))

		// Summary rows carry the exact tallies; per-side row totals
		// derive from compared + one-side-only.
		Expect(md).To(ContainSubstring("| nasa_power_grid_cells | 3 | 4 | 2 | 1 | 1 | 1 | 2 |"))
		Expect(md).To(ContainSubstring("| station_power_cell | 3 | 3 | 3 | 3 | 0 | 0 | 0 |"))
		Expect(md).To(ContainSubstring("Complete power_runs manifests checked: 1; findings: 1"))

		Expect(md).To(ContainSubstring("### Value diffs (1)"))
		Expect(md).To(ContainSubstring("| 19.5N_99.3750W | lat | 19.5 | 19.75 |"))
		Expect(md).To(ContainSubstring("### Derived-only rows (1)"))
		Expect(md).To(ContainSubstring("| 25.0N_100.6250W |"))
		Expect(md).To(ContainSubstring("### Stored-only rows (2)"))
		Expect(md).To(ContainSubstring("| 10.0N_100.0000W |"))
		Expect(md).To(ContainSubstring("_showing first 1 of 2_"))

		// The clean table renders no-findings prose instead of sections.
		Expect(md).To(ContainSubstring("No findings: all 3 compared rows identical."))

		// The manifest table renders every column, one run per column;
		// unit_conversions summarizes to its entry count.
		Expect(md).To(ContainSubstring("| Column | run 7 |"))
		Expect(md).To(ContainSubstring("| temporal_mode | monthly |"))
		Expect(md).To(ContainSubstring("| endpoint_url | https://power.example/api |"))
		Expect(md).To(ContainSubstring("| parameters | T2M,RH2M |"))
		Expect(md).To(ContainSubstring("| community | AG |"))
		Expect(md).To(ContainSubstring("| period_start_year | 1991 |"))
		Expect(md).To(ContainSubstring("| period_end_year | 2020 |"))
		Expect(md).To(ContainSubstring("| period_start_date | NULL |"))
		Expect(md).To(ContainSubstring("| period_end_date | NULL |"))
		Expect(md).To(ContainSubstring("| grid_resolution | 0.5x0.625 |"))
		Expect(md).To(ContainSubstring("| solar_conversion | 11.5 |"))
		Expect(md).To(ContainSubstring("| unit_conversions | 2 entries |"))

		Expect(md).To(ContainSubstring("### Manifest findings (1)"))
		Expect(md).To(ContainSubstring("| 7 | parameters | T2M,RH2M | T2M,T2M_MAX |"))
	})

	It("says so when no complete runs exist", func() {
		cmp := &parity.PowerComparison{}
		md := cmp.Markdown()
		Expect(md).To(ContainSubstring("No complete power_runs rows."))
		Expect(md).To(ContainSubstring("No manifest findings: every checked field equals its pinned value."))
	})

	It("renders a faithful comparison with no findings sections", func(ctx SpecContext) {
		cmp := mustComparePower(ctx, seedFaithfulPowerDB())
		cmp.Label = "/db/faithful.db"

		md := cmp.Markdown()
		Expect(md).To(ContainSubstring("| nasa_power_grid_cells | 2 | 2 | 2 | 2 | 0 | 0 | 0 |"))
		Expect(md).To(ContainSubstring("| station_power_cell | 3 | 3 | 3 | 3 | 0 | 0 | 0 |"))
		Expect(md).To(ContainSubstring("Complete power_runs manifests checked: 2; findings: 0"))
		Expect(md).To(ContainSubstring("| parameters | " + strings.Join(power.DefaultParameters(), ",") + " |"))
		Expect(md).To(ContainSubstring("| unit_conversions | 31 entries |"))
		Expect(md).NotTo(ContainSubstring("### "), "a clean comparison must render no findings sections")
	})
})
