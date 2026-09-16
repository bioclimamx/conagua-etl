package power

import (
	"math"
	"strconv"
)

// MonthlyValue is one (parameter, month) climatological mean and the
// number of years that contributed to it. Years is exposed so the
// orchestrator (and downstream QC) can spot months where POWER has
// substantially less coverage than expected — for a 1991-2020 fit
// the expectation is 30; less than ~24 mirrors the WMO 80% rule.
type MonthlyValue struct {
	Mean  float64
	Years int
}

// RollUp folds POWER's monthly time series into climatological
// monthly means: for each parameter, returns 12 (mean, years)
// entries, one per calendar month.
//
// POWER's monthly time-series response keys are exclusively
// "YYYYMM" strings — six digits. Month `13` is POWER's convention
// for the per-year annual mean (e.g. "199213" is the 1992 annual
// mean), which we discard via the m∈[1,12] check in parseYYYYMM.
// Older POWER documentation referenced an "ANN" overall-annual key
// and a 4-digit per-year key, but the current monthly time-series
// endpoint returns neither — verified directly against
// power.larc.nasa.gov 2026-05-16. The function still defensively
// drops anything that isn't a parseable YYYYMM in [startYear,
// endYear] × [01, 12], and skips values equal to the response's
// fill_value (POWER's missing-data sentinel, typically -999).
//
// Returns parameter → month (1..12) → MonthlyValue. A month not
// present in the inner map means "no valid years contributed";
// callers treat that as NULL in monthly_supplement.
func RollUp(resp *Response, startYear, endYear int) map[string]map[int]MonthlyValue {
	out := map[string]map[int]MonthlyValue{}
	if resp == nil {
		return out
	}
	fill := resp.Header.FillValue

	for param, series := range resp.Properties.Parameter {
		if circularDirectionParams[param] {
			out[param] = rollUpCircular(series, startYear, endYear, fill)
			continue
		}
		sums := [13]float64{}
		counts := [13]int{}
		for k, v := range series {
			y, m, ok := parseYYYYMM(k)
			if !ok {
				continue
			}
			if y < startYear || y > endYear {
				continue
			}
			if v == fill {
				continue
			}
			sums[m] += v
			counts[m]++
		}
		monthMap := map[int]MonthlyValue{}
		for m := 1; m <= 12; m++ {
			if counts[m] == 0 {
				continue
			}
			monthMap[m] = MonthlyValue{
				Mean:  sums[m] / float64(counts[m]),
				Years: counts[m],
			}
		}
		out[param] = monthMap
	}
	return out
}

// parseYYYYMM accepts the 6-digit "YYYYMM" form POWER uses for
// monthly time-series keys. Returns (year, month, ok). Anything
// shorter, longer, or non-numeric returns ok=false. In particular,
// month-13 keys — POWER's per-year annual-mean convention (e.g.
// "199213" is 1992's annual mean) — fail the m∈[1,12] bound and
// are dropped by every caller.
func parseYYYYMM(s string) (year, month int, ok bool) {
	if len(s) != 6 {
		return 0, 0, false
	}
	y, err := strconv.Atoi(s[:4])
	if err != nil {
		return 0, 0, false
	}
	m, err := strconv.Atoi(s[4:])
	if err != nil {
		return 0, 0, false
	}
	if m < 1 || m > 12 {
		return 0, 0, false
	}
	return y, m, true
}

// parseYYYYMMDD accepts the 8-digit "YYYYMMDD" form POWER uses for
// daily time-series keys. Returns ("YYYY-MM-DD", ok). Calendar
// validity (Feb 30, etc.) is not checked here — POWER returns valid
// calendar dates only, verified against the live endpoint
// 2026-05-16, and the daily endpoint has no per-year annual-mean
// convention (no analog of monthly's YYYY13 keys); month and day
// are bounded to plausible ranges defensively. Anything else fails.
func parseYYYYMMDD(s string) (iso string, ok bool) {
	if len(s) != 8 {
		return "", false
	}
	for _, ch := range s {
		if ch < '0' || ch > '9' {
			return "", false
		}
	}
	m, _ := strconv.Atoi(s[4:6])
	d, _ := strconv.Atoi(s[6:8])
	if m < 1 || m > 12 || d < 1 || d > 31 {
		return "", false
	}
	return s[:4] + "-" + s[4:6] + "-" + s[6:8], true
}

// ExtractDaily folds POWER's daily time series into a date-keyed map
// per parameter. For each parameter, returns date ("YYYY-MM-DD") →
// raw POWER value, with fill-value entries dropped and out-of-window
// entries filtered. Unit conversions are NOT applied here — the
// supplement writer applies the same Registry factors the monthly
// path uses, so callers see POWER's native value here and the stored
// value at the writer.
//
// startDate / endDate must be "YYYY-MM-DD". Out-of-window keys are
// dropped defensively in case POWER ever returns dates outside the
// `start=YYYYMMDD&end=YYYYMMDD` window we requested.
func ExtractDaily(resp *Response, startDate, endDate string) map[string]map[string]float64 {
	out := map[string]map[string]float64{}
	if resp == nil {
		return out
	}
	fill := resp.Header.FillValue

	for param, series := range resp.Properties.Parameter {
		days := map[string]float64{}
		for k, v := range series {
			iso, ok := parseYYYYMMDD(k)
			if !ok {
				continue
			}
			if iso < startDate || iso > endDate {
				continue
			}
			if v == fill {
				continue
			}
			days[iso] = v
		}
		out[param] = days
	}
	return out
}

// rollUpCircular folds a per-year monthly time series of angles
// (in degrees) into climatological monthly means using vector
// averaging: for each calendar month, the values that pass the
// window and fill checks are collected in encounter order and folded
// by CircularMeanDegrees.
func rollUpCircular(series map[string]float64, startYear, endYear int, fill float64) map[int]MonthlyValue {
	months := [13][]float64{}
	for k, v := range series {
		y, m, ok := parseYYYYMM(k)
		if !ok {
			continue
		}
		if y < startYear || y > endYear {
			continue
		}
		if v == fill {
			continue
		}
		months[m] = append(months[m], v)
	}
	out := map[int]MonthlyValue{}
	for m := 1; m <= 12; m++ {
		if len(months[m]) == 0 {
			continue
		}
		out[m] = MonthlyValue{Mean: CircularMeanDegrees(months[m]), Years: len(months[m])}
	}
	return out
}

// CircularMeanDegrees is the vector (circular) mean of angles in
// degrees: the (sin θ, cos θ) components are summed in the order given,
// divided by the count, and the angle recovered with atan2. It is the
// one wind-direction mean in the ETL — the monthly rollup and the
// export layer's annual slot both use it — and it is pinned to the
// bit, boundary included: atan2's (-π, π] is
// folded by adding 360 to a negative result, and a near-perfect
// cancellation (5° and 355°) yields a negative angle so small that the
// fold lands on exactly 360.0. The range is therefore the closed
// [0, 360], never normalized to [0, 360); consumers treat 360 and 0 as
// the same direction. An empty input has no mean and yields NaN;
// callers guard that case.
func CircularMeanDegrees(degrees []float64) float64 {
	var sumSin, sumCos float64
	for _, v := range degrees {
		r := v * math.Pi / 180.0
		sumSin += math.Sin(r)
		sumCos += math.Cos(r)
	}
	n := float64(len(degrees))
	deg := math.Atan2(sumSin/n, sumCos/n) * 180.0 / math.Pi
	if deg < 0 {
		deg += 360.0
	}
	return deg
}
