package power

// Param is one row of the POWER parameter registry: the POWER API
// parameter name, the supplement column it lands in, the NASA product
// the values are derived from, POWER's native unit and the unit we
// store, the multiplicative factor applied once at the writer, and
// whether monthly rollup must use the circular (vector) mean instead of
// the arithmetic one.
type Param struct {
	Name       string
	Column     string
	Upstream   string
	PowerUnit  string
	StoredUnit string
	Factor     float64
	Circular   bool
}

// UpstreamMERRA2 and UpstreamCERES name the two NASA products POWER
// derives the 31-parameter set from — MERRA-2 reanalysis and CERES
// SYN1deg — as the deposit's NOTICE, README and abstract credit them.
//
// Lineage is recorded per row because it cannot be inferred from any
// other field. It does not follow the unit: CLOUD_AMT is served in "%"
// and ALLSKY_KT dimensionless, yet both are CERES, while the only other
// "%" row (RH2M) is MERRA-2. It does follow POWER's per-variable start
// dates — the MERRA-2 cohort begins 1981-01-01, the CERES cohorts
// 1984-01-01 (shortwave + cloud), 1988-01-01 (longwave) and 2001-01-01
// (UV + clearness) — which is how each row's value was established, by
// a direct probe of the live daily endpoint.
const (
	UpstreamMERRA2 = "MERRA-2"
	UpstreamCERES  = "CERES"
)

// UnitMJPerM2Day is POWER's native unit for the radiation streams under
// the AG community — exactly the rows SolarMJpm2dToWm2 converts, and
// nothing more. It is a unit, not a lineage: the CERES set also holds
// rows POWER serves in "%" and dimensionless, so the upstream credit
// reads Param.Upstream and never this constant.
const UnitMJPerM2Day = "MJ/m^2/day"

// SolarMJpm2dToWm2 converts POWER's documented unit for the radiation
// streams under the AG community — MJ/m²/day, mean over the day — to
// the W/m² (also a 24-h average) Bioclima stores per the SI-metric
// convention.
//
// Conversion: 1 MJ/m²/day = 1e6 J/m² ÷ 86400 s = 1e6/86400 W/m²
// ≈ 11.574 W/m². The exact factor is recorded in
// power_runs.solar_conversion so downstream consumers can invert.
const SolarMJpm2dToWm2 = 1e6 / 86400.0

// ConvertSolar applies SolarMJpm2dToWm2 to a single value. Trivial
// arithmetic, but exposing it as a function pins the semantics in
// one place and makes the unit conversion testable in isolation.
func ConvertSolar(mjPerM2PerDay float64) float64 {
	return mjPerM2PerDay * SolarMJpm2dToWm2
}

// Registry is the single source of truth for the POWER parameter set:
// DefaultParameters, the Conversions manifest, the supplement INSERT
// column lists, the circular-mean set, and the upstream-product credit
// are all derived from it. Adding a POWER variable is one row here plus
// one DDL column.
//
// Order is load-bearing and fixed, grouped by domain (temperature →
// humidity → wind → shortwave radiation → longwave → sky / atmosphere
// → moisture / evapotranspiration → soil) so the manifest reads as a
// coherent climate description. The comma-joined name list is stored
// verbatim in power_runs.parameters, and the URL a reproducer pastes
// into POWER must produce numerically identical results to ours.
//
// 31 parameters total — exceeds POWER's per-request caps, so the
// orchestrator's per-cell fetch uses Client.FetchBatched, which
// transparently splits and merges. See client.go.
//
// Identity conversions (factor 1.0) are kept for self-description —
// the run manifest tells the full story without consulting POWER
// docs. All radiation streams arrive in MJ/m²/day from the AG
// community and are converted to W/m² with the same factor;
// everything else is stored in POWER's native unit.
var Registry = []Param{
	// Temperature — native °C.
	{Name: "T2M", Column: "t2m_c", Upstream: UpstreamMERRA2, PowerUnit: "C", StoredUnit: "C", Factor: 1.0},
	{Name: "T2M_MAX", Column: "t2m_max_c", Upstream: UpstreamMERRA2, PowerUnit: "C", StoredUnit: "C", Factor: 1.0},
	{Name: "T2M_MIN", Column: "t2m_min_c", Upstream: UpstreamMERRA2, PowerUnit: "C", StoredUnit: "C", Factor: 1.0},
	{Name: "T2MWET", Column: "t2m_wet_c", Upstream: UpstreamMERRA2, PowerUnit: "C", StoredUnit: "C", Factor: 1.0},
	{Name: "T2MDEW", Column: "t2m_dew_c", Upstream: UpstreamMERRA2, PowerUnit: "C", StoredUnit: "C", Factor: 1.0},
	{Name: "TS", Column: "ts_c", Upstream: UpstreamMERRA2, PowerUnit: "C", StoredUnit: "C", Factor: 1.0},
	{Name: "TS_MAX", Column: "ts_max_c", Upstream: UpstreamMERRA2, PowerUnit: "C", StoredUnit: "C", Factor: 1.0},
	{Name: "TS_MIN", Column: "ts_min_c", Upstream: UpstreamMERRA2, PowerUnit: "C", StoredUnit: "C", Factor: 1.0},
	// Humidity — native %, g/kg.
	{Name: "RH2M", Column: "rh2m_pct", Upstream: UpstreamMERRA2, PowerUnit: "%", StoredUnit: "%", Factor: 1.0},
	{Name: "QV2M", Column: "qv2m_gkg", Upstream: UpstreamMERRA2, PowerUnit: "g/kg", StoredUnit: "g/kg", Factor: 1.0},
	// Wind — native m/s, °. Directions are angles: monthly rollup uses
	// the circular (vector) mean for them — the arithmetic mean of 5°
	// and 355° is 180°, nonsense; it should be ~0°.
	{Name: "WS2M", Column: "ws2m_ms", Upstream: UpstreamMERRA2, PowerUnit: "m/s", StoredUnit: "m/s", Factor: 1.0},
	{Name: "WS10M", Column: "ws10m_ms", Upstream: UpstreamMERRA2, PowerUnit: "m/s", StoredUnit: "m/s", Factor: 1.0},
	{Name: "WS50M", Column: "ws50m_ms", Upstream: UpstreamMERRA2, PowerUnit: "m/s", StoredUnit: "m/s", Factor: 1.0},
	{Name: "WD2M", Column: "wd2m_deg", Upstream: UpstreamMERRA2, PowerUnit: "Degrees", StoredUnit: "Degrees", Factor: 1.0, Circular: true},
	{Name: "WD10M", Column: "wd10m_deg", Upstream: UpstreamMERRA2, PowerUnit: "Degrees", StoredUnit: "Degrees", Factor: 1.0, Circular: true},
	// Solar — shortwave. MJ/m²/day → W/m².
	{Name: "ALLSKY_SFC_SW_DWN", Column: "solar_ghi_wm2", Upstream: UpstreamCERES, PowerUnit: UnitMJPerM2Day, StoredUnit: "W/m^2", Factor: SolarMJpm2dToWm2},
	{Name: "ALLSKY_SFC_SW_DIFF", Column: "solar_dhi_wm2", Upstream: UpstreamCERES, PowerUnit: UnitMJPerM2Day, StoredUnit: "W/m^2", Factor: SolarMJpm2dToWm2},
	{Name: "ALLSKY_SFC_SW_DNI", Column: "solar_dni_wm2", Upstream: UpstreamCERES, PowerUnit: UnitMJPerM2Day, StoredUnit: "W/m^2", Factor: SolarMJpm2dToWm2},
	{Name: "CLRSKY_SFC_SW_DWN", Column: "solar_clrsky_wm2", Upstream: UpstreamCERES, PowerUnit: UnitMJPerM2Day, StoredUnit: "W/m^2", Factor: SolarMJpm2dToWm2},
	{Name: "ALLSKY_KT", Column: "clearness_index", Upstream: UpstreamCERES, PowerUnit: "dimensionless", StoredUnit: "dimensionless", Factor: 1.0},
	{Name: "ALLSKY_SFC_PAR_TOT", Column: "par_wm2", Upstream: UpstreamCERES, PowerUnit: UnitMJPerM2Day, StoredUnit: "W/m^2", Factor: SolarMJpm2dToWm2},
	{Name: "ALLSKY_SFC_UVA", Column: "uva_wm2", Upstream: UpstreamCERES, PowerUnit: UnitMJPerM2Day, StoredUnit: "W/m^2", Factor: SolarMJpm2dToWm2},
	{Name: "ALLSKY_SFC_UVB", Column: "uvb_wm2", Upstream: UpstreamCERES, PowerUnit: UnitMJPerM2Day, StoredUnit: "W/m^2", Factor: SolarMJpm2dToWm2},
	// Solar — longwave. MJ/m²/day → W/m².
	{Name: "ALLSKY_SFC_LW_DWN", Column: "lw_dwn_wm2", Upstream: UpstreamCERES, PowerUnit: UnitMJPerM2Day, StoredUnit: "W/m^2", Factor: SolarMJpm2dToWm2},
	// Sky / cloud / atmosphere — native %, kPa. CLOUD_AMT is a CERES
	// product despite its "%" unit: it starts 1984-01-01 with the
	// shortwave streams, not 1981-01-01 with the MERRA-2 meteorology.
	{Name: "CLOUD_AMT", Column: "cloud_amt_pct", Upstream: UpstreamCERES, PowerUnit: "%", StoredUnit: "%", Factor: 1.0},
	{Name: "PS", Column: "ps_kpa", Upstream: UpstreamMERRA2, PowerUnit: "kPa", StoredUnit: "kPa", Factor: 1.0},
	// Moisture and evapotranspiration — native mm/day.
	{Name: "PRECTOTCORR", Column: "precip_mmpd", Upstream: UpstreamMERRA2, PowerUnit: "mm/day", StoredUnit: "mm/day", Factor: 1.0},
	{Name: "EVLAND", Column: "evland_mmpd", Upstream: UpstreamMERRA2, PowerUnit: "mm/day", StoredUnit: "mm/day", Factor: 1.0},
	// Soil moisture — dimensionless 0..1.
	{Name: "GWETTOP", Column: "gwet_top", Upstream: UpstreamMERRA2, PowerUnit: "1", StoredUnit: "1", Factor: 1.0},
	{Name: "GWETROOT", Column: "gwet_root", Upstream: UpstreamMERRA2, PowerUnit: "1", StoredUnit: "1", Factor: 1.0},
	{Name: "GWETPROF", Column: "gwet_prof", Upstream: UpstreamMERRA2, PowerUnit: "1", StoredUnit: "1", Factor: 1.0},
}

// DefaultParameters returns the canonical POWER parameter list the
// puller requests, in Registry order. A fresh slice per call so a
// caller mutating its copy cannot corrupt the registry order every
// manifest depends on.
func DefaultParameters() []string {
	out := make([]string, len(Registry))
	for i, p := range Registry {
		out[i] = p.Name
	}
	return out
}

// ColumnsFrom returns the supplement columns whose values come from the
// named upstream product (UpstreamCERES or UpstreamMERRA2), in Registry
// order, as a fresh slice. It is the one way a consumer gets the
// CERES / MERRA-2 split the deposit credits: the membership is registry
// data, never re-derived from a row's unit, which does not track
// lineage (CLOUD_AMT is CERES yet served in percent). An unrecognised
// upstream returns no columns.
func ColumnsFrom(upstream string) []string {
	var out []string
	for _, p := range Registry {
		if p.Upstream == upstream {
			out = append(out, p.Column)
		}
	}
	return out
}

// UnitConversion documents how one POWER parameter's native unit maps
// to the unit stored in the supplement tables. Recorded per-run in
// power_runs.unit_conversions so a researcher can invert any stored
// value back to POWER's native unit without consulting external docs.
type UnitConversion struct {
	PowerUnit  string  `json:"power_unit"`
	StoredUnit string  `json:"stored_unit"`
	Factor     float64 `json:"factor"`
}

// Conversions is the per-parameter unit mapping for every POWER
// parameter the puller knows about, derived from the Registry.
var Conversions = func() map[string]UnitConversion {
	m := make(map[string]UnitConversion, len(Registry))
	for _, p := range Registry {
		m[p.Name] = UnitConversion{PowerUnit: p.PowerUnit, StoredUnit: p.StoredUnit, Factor: p.Factor}
	}
	return m
}()

// circularDirectionParams is the set of POWER parameters whose values
// are angles in degrees, derived from the Registry. RollUp averages
// these with the circular (vector) mean — see rollUpCircular and the
// WD entries above (references: WMO No. 8 §5, Mardia 1972, NCAR NCL
// wind_stats).
var circularDirectionParams = func() map[string]bool {
	m := map[string]bool{}
	for _, p := range Registry {
		if p.Circular {
			m[p.Name] = true
		}
	}
	return m
}()
