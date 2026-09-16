package publish

// Characterization of the derived annual against CONAGUA's own published
// annual column over a raw normals snapshot — the parser drops that
// column (ingest keeps months 1–12 only), so the spec reads it from the
// NORMAL line of each section directly. It pins what the recompute
// reproduces: every sum (precip_mm, evap_mm) and every mean that does
// not sit on an exact decimal tie, both to the last digit; on a tie —
// the twelve tenths sum to 6 mod 12, so the exact mean ends in 5 at the
// second decimal — CONAGUA's rounding follows no decimal rule this
// recompute could adopt, and the derived annual may differ from the
// published one by 0.1 on such a tie. A formatter
// or aggregation change that broke either exact class would fail here.
// Env-guarded like the parity harnesses: ANNUAL_RAW_SNAPSHOT_DIR names a
// pull snapshot root holding normals_<start>_<end>/<station>.txt.

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/conagua"
)

// annualBySection maps each normals section title to the export name
// its NORMAL row is published under.
var annualBySection = map[string]string{
	"TEMPERATURA MÁXIMA": ExportName("monthly_normals", "tmax"),
	"TEMPERATURA MÍNIMA": ExportName("monthly_normals", "tmin"),
	"TEMPERATURA MEDIA":  ExportName("monthly_normals", "tmean"),
	"PRECIPITACIÓN":      ExportName("monthly_normals", "precip"),
	"EVAPORACIÓN":        ExportName("monthly_normals", "evap"),
}

// publishedAnnuals reads the trailing annual token of each section's
// NORMAL line: the label, then twelve month tokens, then the annual;
// a row with fewer than thirteen non-empty tokens publishes no annual.
func publishedAnnuals(path string) (map[string]float64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close() //nolint:errcheck // read-only fixture

	out := map[string]float64{}
	section := ""
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimRight(sc.Text(), "\r")
		if name, ok := annualBySection[strings.TrimSpace(line)]; ok {
			section = name
			continue
		}
		fields := strings.Split(line, "\t")
		if section == "" || strings.TrimSpace(fields[0]) != "NORMAL" {
			continue
		}
		var tokens []string
		for _, raw := range fields[1:] {
			if s := strings.TrimSpace(raw); s != "" {
				tokens = append(tokens, s)
			}
		}
		if len(tokens) == 13 {
			if v, err := strconv.ParseFloat(tokens[12], 64); err == nil {
				out[section] = v
			}
		}
		section = ""
	}
	return out, sc.Err()
}

// tenthsTie reports whether the twelve one-decimal months put the exact
// mean on a decimal tie: their tenths sum to 6 modulo 12.
func tenthsTie(months [12]*float64) bool {
	var total int64
	for _, v := range months {
		total += int64(math.Round(*v * 10))
	}
	return ((total%12)+12)%12 == 6
}

var _ = ginkgo.Describe("the derived annual against CONAGUA's published annual column over a raw snapshot", func() {
	ginkgo.It("reproduces every published sum and every non-tie mean exactly, and stays within 0.1 on a decimal tie", func() {
		root := os.Getenv("ANNUAL_RAW_SNAPSHOT_DIR")
		if root == "" {
			ginkgo.Skip("ANNUAL_RAW_SNAPSHOT_DIR not set")
		}
		dirs, err := filepath.Glob(filepath.Join(root, "normals_*_*"))
		Expect(err).NotTo(HaveOccurred())
		Expect(dirs).NotTo(BeEmpty())

		type tally struct{ exact, tie, tieOff, partial, files int }
		byName := map[string]*tally{}
		for _, name := range annualBySection {
			byName[name] = &tally{}
		}
		for _, dir := range dirs {
			period := strings.ReplaceAll(strings.TrimPrefix(filepath.Base(dir), "normals_"), "_", "-")
			files, err := filepath.Glob(filepath.Join(dir, "*.txt"))
			Expect(err).NotTo(HaveOccurred())
			for _, path := range files {
				published, err := publishedAnnuals(path)
				Expect(err).NotTo(HaveOccurred(), path)
				f, err := os.Open(path)
				Expect(err).NotTo(HaveOccurred())
				_, rows, _, _, err := conagua.ParseNormalsFile(f, period)
				Expect(f.Close()).To(Succeed())
				Expect(err).NotTo(HaveOccurred(), path)
				Expect(rows).To(HaveLen(12), path)

				series := map[string][12]*float64{}
				for i, r := range rows {
					for name, get := range map[string]func(conagua.MonthlyNormalsRow) *float64{
						annualBySection["TEMPERATURA MÁXIMA"]: func(r conagua.MonthlyNormalsRow) *float64 { return r.Tmax },
						annualBySection["TEMPERATURA MÍNIMA"]: func(r conagua.MonthlyNormalsRow) *float64 { return r.Tmin },
						annualBySection["TEMPERATURA MEDIA"]:  func(r conagua.MonthlyNormalsRow) *float64 { return r.Tmean },
						annualBySection["PRECIPITACIÓN"]:      func(r conagua.MonthlyNormalsRow) *float64 { return r.Precip },
						annualBySection["EVAPORACIÓN"]:        func(r conagua.MonthlyNormalsRow) *float64 { return r.Evap },
					} {
						m := series[name]
						m[i] = get(r)
						series[name] = m
					}
				}
				for name, want := range published {
					t := byName[name]
					t.files++
					derived := annualBy[name].fold(series[name])
					if derived == nil {
						t.partial++
						continue
					}
					got := formatReal(*derived, 1)
					wantLit := formatReal(want, 1)
					where := fmt.Sprintf("%s %s", filepath.Base(path), name)
					switch {
					case annualBy[name] == annualSum:
						Expect(got).To(Equal(wantLit), where)
						t.exact++
					case !tenthsTie(series[name]):
						Expect(got).To(Equal(wantLit), where)
						t.exact++
					case got == wantLit:
						t.tie++
					default:
						Expect(math.Abs(*derived-want)).To(BeNumerically("<=", 0.1+1e-9), where)
						t.tieOff++
					}
				}
			}
		}
		for _, name := range []string{"tmax_c", "tmin_c", "tmean_c", "precip_mm", "evap_mm"} {
			t := byName[name]
			ginkgo.GinkgoWriter.Printf("%-10s published %5d: exact %5d, tie agreeing %4d, tie off by 0.1 %4d, partial (no derived annual) %4d\n",
				name, t.files, t.exact, t.tie, t.tieOff, t.partial)
			Expect(t.files).To(BeNumerically(">", 0), name)
		}
		Expect(byName["precip_mm"].tie + byName["precip_mm"].tieOff).To(BeZero())
		Expect(byName["evap_mm"].tie + byName["evap_mm"].tieOff).To(BeZero())
	})
})
