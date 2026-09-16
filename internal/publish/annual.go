package publish

import (
	"fmt"

	"github.com/bioclimamx/conagua-etl/internal/power"
)

// annualKind is the aggregation that fills slot 12 of a 13-slot month
// series: recomputed per variable at export from the stored
// full-precision values of slots 0–11, unweighted, and tagged
// bioclima_derived. The class follows the quantity, which the export
// name's unit suffix names: a monthly total (precip_mm, evap_mm) sums; an
// intensive or rate variable (temperatures, precip_mmpd, humidity, wind
// speed, solar, pressure, soil, indices) means; a wind direction takes
// the circular mean. Never a uniform arithmetic mean over CONAGUA's
// precip_mm / evap_mm — that would understate the annual total ~12×.
type annualKind int

// The three aggregations.
const (
	annualMean annualKind = iota
	annualSum
	annualCircular
)

// fold applies the aggregation to a variable's twelve months.
func (k annualKind) fold(months [12]*float64) *float64 {
	switch k {
	case annualSum:
		return sumAnnual(months)
	case annualCircular:
		return circularAnnual(months)
	default:
		return meanAnnual(months)
	}
}

// annualBy is the annual dispatch table, keyed by export name: the five
// CONAGUA normals variables listed below, and POWER-31 derived from the
// registry that owns which parameters are angles. It covers every
// variable that carries a derived annual and nothing else; a series whose
// name is absent — every monthly_normals_extras column — keeps slot 12
// null.
var annualBy = func() map[string]annualKind {
	m := map[string]annualKind{
		ExportName("monthly_normals", "tmax"):   annualMean,
		ExportName("monthly_normals", "tmin"):   annualMean,
		ExportName("monthly_normals", "tmean"):  annualMean,
		ExportName("monthly_normals", "precip"): annualSum,
		ExportName("monthly_normals", "evap"):   annualSum,
	}
	for _, p := range power.Registry {
		if _, dup := m[p.Column]; dup {
			panic(fmt.Sprintf("publish: annual dispatch names %s twice", p.Column))
		}
		if p.Circular {
			m[p.Column] = annualCircular
		} else {
			m[p.Column] = annualMean
		}
	}
	return m
}()

// complete returns the twelve values in slot order when every month is
// present. Slot 12 is null unless all twelve months are non-null: a
// partial sum understates a total and a partial mean is seasonally
// biased, and an honest gap is null, never invented.
func complete(months [12]*float64) ([]float64, bool) {
	vals := make([]float64, 0, len(months))
	for _, v := range months {
		if v == nil {
			return nil, false
		}
		vals = append(vals, *v)
	}
	return vals, true
}

// sumAnnual is the annual total of a monthly-total variable, summed in
// slot order.
func sumAnnual(months [12]*float64) *float64 {
	vals, ok := complete(months)
	if !ok {
		return nil
	}
	var sum float64
	for _, v := range vals {
		sum += v
	}
	return &sum
}

// meanAnnual is the unweighted mean of the twelve months — CONAGUA's own
// published convention (328.4 / 12 = 27.4 on the 1991-2020 fixture).
func meanAnnual(months [12]*float64) *float64 {
	sum := sumAnnual(months)
	if sum == nil {
		return nil
	}
	mean := *sum / float64(len(months))
	return &mean
}

// circularAnnual is the vector mean of the twelve monthly directions,
// through the one wind-direction mean the ETL has
// (power.CircularMeanDegrees) — closed [0, 360] boundary included.
func circularAnnual(months [12]*float64) *float64 {
	vals, ok := complete(months)
	if !ok {
		return nil
	}
	mean := power.CircularMeanDegrees(vals)
	return &mean
}
