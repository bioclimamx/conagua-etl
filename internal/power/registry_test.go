package power_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"regexp"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/power"
)

// pinnedParameterOrder is the pinned DefaultParameters list, verbatim.
// Order is the reproducibility contract: comma-joined it is
// byte-identical to the national DB's power_runs.parameters manifest,
// and it is the list a reproducer pastes into a POWER URL.
var pinnedParameterOrder = []string{
	// Temperature
	"T2M", "T2M_MAX", "T2M_MIN", "T2MWET", "T2MDEW",
	"TS", "TS_MAX", "TS_MIN",
	// Humidity
	"RH2M", "QV2M",
	// Wind
	"WS2M", "WS10M", "WS50M", "WD2M", "WD10M",
	// Solar — shortwave
	"ALLSKY_SFC_SW_DWN", "ALLSKY_SFC_SW_DIFF", "ALLSKY_SFC_SW_DNI",
	"CLRSKY_SFC_SW_DWN", "ALLSKY_KT",
	"ALLSKY_SFC_PAR_TOT", "ALLSKY_SFC_UVA", "ALLSKY_SFC_UVB",
	// Solar — longwave
	"ALLSKY_SFC_LW_DWN",
	// Sky / cloud / atmosphere
	"CLOUD_AMT", "PS",
	// Moisture and evapotranspiration
	"PRECTOTCORR", "EVLAND",
	// Soil moisture
	"GWETTOP", "GWETROOT", "GWETPROF",
}

// pinnedConversions is the pinned Conversions map, verbatim —
// the unit strings are published in power_runs.unit_conversions, so
// any drift here breaks manifest parity with the national DB.
var pinnedConversions = map[string]power.UnitConversion{
	// Temperature — native °C.
	"T2M":     {PowerUnit: "C", StoredUnit: "C", Factor: 1.0},
	"T2M_MAX": {PowerUnit: "C", StoredUnit: "C", Factor: 1.0},
	"T2M_MIN": {PowerUnit: "C", StoredUnit: "C", Factor: 1.0},
	"T2MWET":  {PowerUnit: "C", StoredUnit: "C", Factor: 1.0},
	"T2MDEW":  {PowerUnit: "C", StoredUnit: "C", Factor: 1.0},
	"TS":      {PowerUnit: "C", StoredUnit: "C", Factor: 1.0},
	"TS_MAX":  {PowerUnit: "C", StoredUnit: "C", Factor: 1.0},
	"TS_MIN":  {PowerUnit: "C", StoredUnit: "C", Factor: 1.0},

	// Humidity — native %, g/kg.
	"RH2M": {PowerUnit: "%", StoredUnit: "%", Factor: 1.0},
	"QV2M": {PowerUnit: "g/kg", StoredUnit: "g/kg", Factor: 1.0},

	// Wind — native m/s, °.
	"WS2M":  {PowerUnit: "m/s", StoredUnit: "m/s", Factor: 1.0},
	"WS10M": {PowerUnit: "m/s", StoredUnit: "m/s", Factor: 1.0},
	"WS50M": {PowerUnit: "m/s", StoredUnit: "m/s", Factor: 1.0},
	"WD2M":  {PowerUnit: "Degrees", StoredUnit: "Degrees", Factor: 1.0},
	"WD10M": {PowerUnit: "Degrees", StoredUnit: "Degrees", Factor: 1.0},

	// Solar — MJ/m²/day → W/m².
	"ALLSKY_SFC_SW_DWN":  {PowerUnit: "MJ/m^2/day", StoredUnit: "W/m^2", Factor: power.SolarMJpm2dToWm2},
	"ALLSKY_SFC_SW_DIFF": {PowerUnit: "MJ/m^2/day", StoredUnit: "W/m^2", Factor: power.SolarMJpm2dToWm2},
	"ALLSKY_SFC_SW_DNI":  {PowerUnit: "MJ/m^2/day", StoredUnit: "W/m^2", Factor: power.SolarMJpm2dToWm2},
	"CLRSKY_SFC_SW_DWN":  {PowerUnit: "MJ/m^2/day", StoredUnit: "W/m^2", Factor: power.SolarMJpm2dToWm2},
	"ALLSKY_SFC_PAR_TOT": {PowerUnit: "MJ/m^2/day", StoredUnit: "W/m^2", Factor: power.SolarMJpm2dToWm2},
	"ALLSKY_SFC_UVA":     {PowerUnit: "MJ/m^2/day", StoredUnit: "W/m^2", Factor: power.SolarMJpm2dToWm2},
	"ALLSKY_SFC_UVB":     {PowerUnit: "MJ/m^2/day", StoredUnit: "W/m^2", Factor: power.SolarMJpm2dToWm2},
	"ALLSKY_SFC_LW_DWN":  {PowerUnit: "MJ/m^2/day", StoredUnit: "W/m^2", Factor: power.SolarMJpm2dToWm2},
	"ALLSKY_KT":          {PowerUnit: "dimensionless", StoredUnit: "dimensionless", Factor: 1.0},

	// Sky / cloud / atmosphere — native %, kPa.
	"CLOUD_AMT": {PowerUnit: "%", StoredUnit: "%", Factor: 1.0},
	"PS":        {PowerUnit: "kPa", StoredUnit: "kPa", Factor: 1.0},

	// Moisture and evapotranspiration — native mm/day.
	"PRECTOTCORR": {PowerUnit: "mm/day", StoredUnit: "mm/day", Factor: 1.0},
	"EVLAND":      {PowerUnit: "mm/day", StoredUnit: "mm/day", Factor: 1.0},

	// Soil moisture — dimensionless 0..1.
	"GWETTOP":  {PowerUnit: "1", StoredUnit: "1", Factor: 1.0},
	"GWETROOT": {PowerUnit: "1", StoredUnit: "1", Factor: 1.0},
	"GWETPROF": {PowerUnit: "1", StoredUnit: "1", Factor: 1.0},
}

// valueColumns returns table's column names in DDL (cid) order with the
// key/backpointer columns stripped — exactly the columns whose order
// must match the registry.
func valueColumns(db *sql.DB, table string) []string {
	GinkgoHelper()
	keyCols := map[string]bool{
		"cell_id": true, "period": true, "month": true, "date": true, "power_run_id": true,
	}
	rows, err := db.Query(`SELECT name FROM pragma_table_info(?) ORDER BY cid`, table)
	Expect(err).NotTo(HaveOccurred())
	defer rows.Close() //nolint:errcheck // read-side close in a spec
	var out []string
	for rows.Next() {
		var name string
		Expect(rows.Scan(&name)).To(Succeed())
		if !keyCols[name] {
			out = append(out, name)
		}
	}
	Expect(rows.Err()).NotTo(HaveOccurred())
	return out
}

var _ = Describe("The parameter registry", func() {
	It("carries the pinned 31-parameter manifest order", func() {
		Expect(power.Registry).To(HaveLen(31))
		Expect(power.DefaultParameters()).To(Equal(pinnedParameterOrder))
	})

	It("derives DefaultParameters from the Registry name sequence, as a fresh slice", func() {
		names := make([]string, len(power.Registry))
		for i, p := range power.Registry {
			names[i] = p.Name
		}
		Expect(power.DefaultParameters()).To(Equal(names))

		mutated := power.DefaultParameters()
		mutated[0] = "MUTATED"
		Expect(power.DefaultParameters()).To(Equal(names),
			"a caller mutating its copy must not corrupt the manifest order")
	})

	It("matches the pinned Conversions map verbatim", func() {
		Expect(power.Conversions).To(Equal(pinnedConversions))
	})

	It("derives Conversions from the Registry, entry for entry", func() {
		Expect(power.Conversions).To(HaveLen(len(power.Registry)))
		for _, p := range power.Registry {
			Expect(power.Conversions).To(HaveKeyWithValue(p.Name, power.UnitConversion{
				PowerUnit:  p.PowerUnit,
				StoredUnit: p.StoredUnit,
				Factor:     p.Factor,
			}), p.Name)
		}
	})

	It("carries the exact solar factor on radiation entries and identity elsewhere", func() {
		for _, p := range power.Registry {
			if p.StoredUnit == "W/m^2" {
				// Float-exact — the manifest's factor must match the
				// national DB's byte for byte.
				Expect(p.Factor).To(Equal(power.SolarMJpm2dToWm2), p.Name)
			} else {
				Expect(p.Factor).To(Equal(1.0), p.Name)
			}
		}
	})

	It("flags exactly WD2M and WD10M as circular", func() {
		var circular []string
		for _, p := range power.Registry {
			if p.Circular {
				circular = append(circular, p.Name)
			}
		}
		Expect(circular).To(Equal([]string{"WD2M", "WD10M"}))
	})
})

// The registry's Go side and schema.sql's column lists cannot reference
// each other, so these specs are the lockstep tripwire: the registry's
// 31 columns must equal each
// supplement table's value columns in DDL order, and the grid constant
// must equal the DDL's CHECK literal.
var _ = Describe("DDL lockstep with the parameter registry", func() {
	registryColumns := func() []string {
		cols := make([]string, len(power.Registry))
		for i, p := range power.Registry {
			cols[i] = p.Column
		}
		return cols
	}

	It("matches monthly_supplement's value columns in DDL order", func() {
		Expect(valueColumns(openTestDB(), "monthly_supplement")).To(Equal(registryColumns()))
	})

	It("matches daily_supplement's value columns in DDL order", func() {
		Expect(valueColumns(openTestDB(), "daily_supplement")).To(Equal(registryColumns()))
	})

	It("pins Resolution to the grid_resolution CHECK literal", func() {
		ddl, err := os.ReadFile(filepath.Join("..", "schema", "schema.sql"))
		Expect(err).NotTo(HaveOccurred())

		checkRe := regexp.MustCompile(`(?s)CHECK \(grid_resolution IN \((.*?)\)\)`)
		m := checkRe.FindSubmatch(ddl)
		Expect(m).NotTo(BeNil(), "nasa_power_grid_cells must carry a grid_resolution CHECK")

		var literals []string
		for _, lit := range regexp.MustCompile(`'([^']+)'`).FindAllSubmatch(m[1], -1) {
			literals = append(literals, string(lit[1]))
		}
		Expect(literals).To(Equal([]string{power.Resolution}))
	})
})

// probedUpstreamCohorts transcribes a "POWER per-variable temporal
// coverage" table established by direct probe of the live daily
// endpoint on 2026-05-16. It is this spec's independent oracle for
// Param.Upstream:
// the cohort POWER's own availability date puts a variable in *is* its
// upstream product. An expectation derived from any registry field
// would only restate the code — the tautology that would let CLOUD_AMT,
// a CERES product served in "%", be credited to MERRA-2 unnoticed.
var probedUpstreamCohorts = []struct {
	AvailableFrom string
	Upstream      string
	Names         []string
}{
	{"1981-01-01", power.UpstreamMERRA2, []string{
		"T2M", "T2M_MAX", "T2M_MIN", "T2MWET", "T2MDEW", "TS", "TS_MAX", "TS_MIN",
		"RH2M", "QV2M", "WS2M", "WS10M", "WS50M", "WD2M", "WD10M", "PS",
		"PRECTOTCORR", "EVLAND", "GWETTOP", "GWETROOT", "GWETPROF",
	}},
	{"1984-01-01", power.UpstreamCERES, []string{
		"ALLSKY_SFC_SW_DWN", "ALLSKY_SFC_SW_DIFF", "ALLSKY_SFC_SW_DNI",
		"CLRSKY_SFC_SW_DWN", "ALLSKY_SFC_PAR_TOT", "CLOUD_AMT",
	}},
	{"1988-01-01", power.UpstreamCERES, []string{"ALLSKY_SFC_LW_DWN"}},
	{"2001-01-01", power.UpstreamCERES, []string{"ALLSKY_SFC_UVA", "ALLSKY_SFC_UVB", "ALLSKY_KT"}},
}

// probedUpstreamOf flattens the cohort table into a per-parameter
// lookup, failing the spec if a parameter is ever listed in two cohorts.
func probedUpstreamOf() map[string]string {
	GinkgoHelper()
	out := map[string]string{}
	for _, cohort := range probedUpstreamCohorts {
		for _, name := range cohort.Names {
			Expect(out).NotTo(HaveKey(name), "%s appears in two coverage cohorts", name)
			out[name] = cohort.Upstream
		}
	}
	return out
}

// paramByName finds a registry row, failing the spec when the parameter
// is absent rather than asserting against a zero-valued Param.
func paramByName(name string) power.Param {
	GinkgoHelper()
	for _, p := range power.Registry {
		if p.Name == name {
			return p
		}
	}
	Fail("no registry row named " + name)
	return power.Param{}
}

var _ = Describe("Upstream provenance — the CERES / MERRA-2 split", func() {
	var oracle map[string]string

	BeforeEach(func() {
		oracle = probedUpstreamOf()
	})

	It("covers exactly the registry's 31 parameters", func() {
		Expect(oracle).To(HaveLen(len(power.Registry)))
		for _, p := range power.Registry {
			Expect(oracle).To(HaveKey(p.Name))
		}
	})

	It("credits every row to the product the upstream coverage probe placed it in", func() {
		for _, p := range power.Registry {
			Expect(p.Upstream).To(Equal(oracle[p.Name]), p.Name)
		}
	})

	It("carries a known upstream on every row, so a new row cannot silently join neither set", func() {
		for _, p := range power.Registry {
			Expect(p.Upstream).To(BeElementOf(power.UpstreamCERES, power.UpstreamMERRA2), p.Name)
		}
	})

	It("splits the 31 parameters 10 CERES / 21 MERRA-2", func() {
		var ceres, merra int
		for _, name := range power.DefaultParameters() {
			switch oracle[name] {
			case power.UpstreamCERES:
				ceres++
			case power.UpstreamMERRA2:
				merra++
			}
		}
		Expect(ceres).To(Equal(10))
		Expect(merra).To(Equal(21))
		Expect(power.ColumnsFrom(power.UpstreamCERES)).To(HaveLen(ceres))
		Expect(power.ColumnsFrom(power.UpstreamMERRA2)).To(HaveLen(merra))
	})

	It("lists the CERES columns in registry order, cloud_amt_pct last", func() {
		ceres := power.ColumnsFrom(power.UpstreamCERES)
		Expect(ceres).To(Equal([]string{
			"solar_ghi_wm2", "solar_dhi_wm2", "solar_dni_wm2", "solar_clrsky_wm2", "clearness_index",
			"par_wm2", "uva_wm2", "uvb_wm2", "lw_dwn_wm2", "cloud_amt_pct",
		}))
		Expect(ceres[len(ceres)-1]).To(Equal("cloud_amt_pct"))
	})

	It("lists the MERRA-2 columns in registry order", func() {
		Expect(power.ColumnsFrom(power.UpstreamMERRA2)).To(Equal([]string{
			"t2m_c", "t2m_max_c", "t2m_min_c", "t2m_wet_c", "t2m_dew_c", "ts_c", "ts_max_c", "ts_min_c",
			"rh2m_pct", "qv2m_gkg", "ws2m_ms", "ws10m_ms", "ws50m_ms", "wd2m_deg", "wd10m_deg",
			"ps_kpa", "precip_mmpd", "evland_mmpd", "gwet_top", "gwet_root", "gwet_prof",
		}))
	})

	It("keeps the two sets disjoint and exhaustive over the registry's columns", func() {
		var all []string
		for _, p := range power.Registry {
			all = append(all, p.Column)
		}
		merged := append(power.ColumnsFrom(power.UpstreamCERES), power.ColumnsFrom(power.UpstreamMERRA2)...)
		Expect(merged).To(ConsistOf(all))
	})

	It("does not track the unit — the classifier that caused the defect", func() {
		// CLOUD_AMT and ALLSKY_KT are CERES products POWER does not
		// serve as a radiation flux; RH2M shares CLOUD_AMT's unit and is
		// MERRA-2. Any rule reading PowerUnit gets at least one wrong.
		cloud := paramByName("CLOUD_AMT")
		Expect(cloud.PowerUnit).To(Equal("%"))
		Expect(cloud.Upstream).To(Equal(power.UpstreamCERES))

		kt := paramByName("ALLSKY_KT")
		Expect(kt.PowerUnit).To(Equal("dimensionless"))
		Expect(kt.Upstream).To(Equal(power.UpstreamCERES))

		rh := paramByName("RH2M")
		Expect(rh.PowerUnit).To(Equal(cloud.PowerUnit))
		Expect(rh.Upstream).To(Equal(power.UpstreamMERRA2))
	})

	It("returns a fresh slice a caller cannot use to corrupt the registry", func() {
		first := power.ColumnsFrom(power.UpstreamCERES)
		want := append([]string(nil), first...)
		first[0] = "MUTATED"
		Expect(power.ColumnsFrom(power.UpstreamCERES)).To(Equal(want))
		Expect(paramByName("ALLSKY_SFC_SW_DWN").Column).To(Equal("solar_ghi_wm2"))
	})

	It("returns no columns for an upstream the registry does not carry", func() {
		Expect(power.ColumnsFrom("MERRA-3")).To(BeEmpty())
		Expect(power.ColumnsFrom("")).To(BeEmpty())
	})
})
