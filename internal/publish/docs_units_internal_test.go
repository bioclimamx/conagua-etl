package publish

// In-package specs for unitOf, the one place a unit is read from a
// column name: every suffix of the map and every whole-name entry, the
// WMO prefix, the names that carry no unit, the near-miss names a
// looser match would misread, the map's no-suffix-of-another invariant
// that makes the first match the only match, and every exported
// numeric of the DDL resolving as its type demands — a REAL to a unit,
// an INTEGER to none — under its export name.

import (
	"strings"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/schema"
)

func unitOrNone(name string) string {
	if u := unitOf(name); u != nil {
		return *u
	}
	return "<none>"
}

// suffixCases is every suffix of the map with an exported column that
// carries it and the unit it pins.
var suffixCases = []struct{ suffix, column, unit string }{
	{"_c", "tmax_c", "°C"},
	{"_mm", "precip_mm", "mm"},
	{"_mmpd", "precip_mmpd", "mm/day"},
	{"_pct", "rh2m_pct", "%"},
	{"_gkg", "qv2m_gkg", "g/kg"},
	{"_ms", "ws10m_ms", "m/s"},
	{"_deg", "wd2m_deg", "degrees"},
	{"_wm2", "solar_ghi_wm2", "W/m²"},
	{"_kpa", "ps_kpa", "kPa"},
	{"_km", "distance_km", "km"},
	{"_m", "altitude_m", "m"},
	{"_days", "rain_days", "days"},
}

var _ = ginkgo.Describe("unitOf — the unit derived from a column name", func() {
	for _, tc := range suffixCases {
		ginkgo.It("resolves "+tc.suffix+" to "+tc.unit+" on a synthetic name and on "+tc.column, func() {
			gomega.Expect(strings.HasSuffix(tc.column, tc.suffix)).To(gomega.BeTrue())
			gomega.Expect(unitOrNone("x" + tc.suffix)).To(gomega.Equal(tc.unit))
			gomega.Expect(unitOrNone(tc.column)).To(gomega.Equal(tc.unit))
		})
	}

	ginkgo.It("has a case above for every suffix of the map, in the map's order, and no other", func() {
		gomega.Expect(unitSuffixes).To(gomega.HaveLen(len(suffixCases)))
		for i, u := range unitSuffixes {
			gomega.Expect(u.Pattern).To(gomega.Equal(suffixCases[i].suffix), "suffix %d", i)
			gomega.Expect(u.Unit).To(gomega.Equal(suffixCases[i].unit), u.Pattern)
		}
	})

	ginkgo.It("resolves the whole-name entries and the WMO prefix", func() {
		gomega.Expect(unitOrNone("lat")).To(gomega.Equal("decimal degrees"))
		gomega.Expect(unitOrNone("lon")).To(gomega.Equal("decimal degrees"))
		gomega.Expect(unitOrNone("clearness_index")).To(gomega.Equal("dimensionless"))
		gomega.Expect(unitOrNone("gwet_top")).To(gomega.Equal("dimensionless"))
		gomega.Expect(unitOrNone("gwet_root")).To(gomega.Equal("dimensionless"))
		gomega.Expect(unitOrNone("gwet_prof")).To(gomega.Equal("dimensionless"))
		for _, w := range wmoByPeriod {
			gomega.Expect(unitOrNone(w.bin)).To(gomega.Equal("dimensionless"), w.bin)
			gomega.Expect(unitOrNone(w.cont)).To(gomega.Equal("dimensionless"), w.cont)
		}
		gomega.Expect(wmoByPeriod).To(gomega.HaveLen(4))
		gomega.Expect(unitNames).To(gomega.HaveLen(7))
	})

	ginkgo.DescribeTable("gives no unit to a month, a year, a count, a key, a date, or a name outside the map",
		func(name string) {
			gomega.Expect(unitOf(name)).To(gomega.BeNil())
		},
		ginkgo.Entry("month", "month"),
		ginkgo.Entry("first_year", "first_year"),
		ginkgo.Entry("last_year", "last_year"),
		ginkgo.Entry("an extreme's year", "tmax_monthly_extreme_year"),
		ginkgo.Entry("a years-with-data count", "precip_years_with_data"),
		ginkgo.Entry("the rain days' count", "rain_days_years_with_data"),
		ginkgo.Entry("stations_attempted", "stations_attempted"),
		ginkgo.Entry("stations_succeeded", "stations_succeeded"),
		ginkgo.Entry("stations_failed", "stations_failed"),
		ginkgo.Entry("daily_rows", "daily_rows"),
		ginkgo.Entry("normals_rows", "normals_rows"),
		ginkgo.Entry("extras_rows", "extras_rows"),
		ginkgo.Entry("warnings_total", "warnings_total"),
		ginkgo.Entry("period_start_year", "period_start_year"),
		ginkgo.Entry("period_end_year", "period_end_year"),
		ginkgo.Entry("cells_attempted", "cells_attempted"),
		ginkgo.Entry("cells_succeeded", "cells_succeeded"),
		ginkgo.Entry("cells_failed", "cells_failed"),
		ginkgo.Entry("supplement_rows", "supplement_rows"),
		ginkgo.Entry("a station key", "station_id"),
		ginkgo.Entry("a cell key", "cell_id"),
		ginkgo.Entry("a date", "date"),
		ginkgo.Entry("a period", "period"),
		ginkgo.Entry("an extras date", "tmax_daily_extreme_date"),
		ginkgo.Entry("the profile's days_with_obs", "days_with_obs"),
		ginkgo.Entry("the bare CONAGUA tmax, before the export rename", "tmax"),
		ginkgo.Entry("the bare CONAGUA precip, before the export rename", "precip"),
		ginkgo.Entry("the WMO prefix without its underscore", "wmo_completeness"),
		ginkgo.Entry("a whole-name entry with a suffix", "clearness_index_x"),
		ginkgo.Entry("an empty name", ""),
	)

	ginkgo.DescribeTable("reads the suffix that is there, never a shorter one a looser match would see inside it",
		func(name, unit string) {
			gomega.Expect(unitOrNone(name)).To(gomega.Equal(unit))
		},
		ginkgo.Entry("precip_mm is mm", "precip_mm", "mm"),
		ginkgo.Entry("evap_mm is mm", "evap_mm", "mm"),
		ginkgo.Entry("precip_mmpd is mm/day, not mm", "precip_mmpd", "mm/day"),
		ginkgo.Entry("evland_mmpd is mm/day", "evland_mmpd", "mm/day"),
		ginkgo.Entry("t2m_c is °C, not m", "t2m_c", "°C"),
		ginkgo.Entry("ws2m_ms is m/s, not m", "ws2m_ms", "m/s"),
		ginkgo.Entry("ws50m_ms is m/s", "ws50m_ms", "m/s"),
		ginkgo.Entry("distance_km is km, not m", "distance_km", "km"),
		ginkgo.Entry("lw_dwn_wm2 is W/m², not m", "lw_dwn_wm2", "W/m²"),
		ginkgo.Entry("qv2m_gkg is g/kg", "qv2m_gkg", "g/kg"),
		ginkgo.Entry("precip_daily_extreme_mm is mm", "precip_daily_extreme_mm", "mm"),
		ginkgo.Entry("tmin_daily_extreme_c is °C", "tmin_daily_extreme_c", "°C"),
		ginkgo.Entry("cloud_amt_pct is %", "cloud_amt_pct", "%"),
	)

	ginkgo.It("lists no suffix that ends another and no whole name a suffix would match, so the first match is the only match", func() {
		for _, a := range unitSuffixes {
			for _, b := range unitSuffixes {
				if a.Pattern != b.Pattern {
					gomega.Expect(strings.HasSuffix(a.Pattern, b.Pattern)).To(gomega.BeFalse(), "%s ends with %s", a.Pattern, b.Pattern)
				}
			}
		}
		seen := map[string]bool{}
		for _, u := range append(append([]DictionaryUnit{}, unitSuffixes...), unitNames...) {
			gomega.Expect(seen).NotTo(gomega.HaveKey(u.Pattern))
			seen[u.Pattern] = true
			gomega.Expect(u.Unit).NotTo(gomega.BeEmpty(), u.Pattern)
		}
		for _, n := range unitNames {
			name := strings.TrimSuffix(n.Pattern, "*")
			for _, s := range unitSuffixes {
				gomega.Expect(strings.HasSuffix(name, s.Pattern)).To(gomega.BeFalse(), "%s would also match %s", n.Pattern, s.Pattern)
			}
		}
	})

	ginkgo.It("resolves every exported numeric of the DDL as its type demands under the renamed export name: a REAL to a unit, an INTEGER to none", func() {
		reals, ints := 0, 0
		for _, t := range schema.Tables() {
			for _, c := range t.Columns {
				if _, pinned := schema.Decimals(t.Name, c.Name); !pinned {
					continue
				}
				u := unitOf(ExportName(t.Name, c.Name))
				switch c.Type {
				case "REAL":
					gomega.Expect(u).NotTo(gomega.BeNil(), "%s.%s", t.Name, c.Name)
					reals++
				case "INTEGER":
					gomega.Expect(u).To(gomega.BeNil(), "%s.%s", t.Name, c.Name)
					ints++
				default:
					ginkgo.Fail(t.Name + "." + c.Name + " is pinned but neither REAL nor INTEGER")
				}
			}
		}
		// The Precision annotation: 92 REAL and 27 INTEGER columns.
		gomega.Expect(reals).To(gomega.Equal(92))
		gomega.Expect(ints).To(gomega.Equal(27))
	})
})
