package power_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/power"
)

// The grid specs characterize the snap-to-grid math. Every expected
// centroid is an integer multiple of 0.5° lat / 0.625° lon and therefore
// exactly representable in binary floating point, so cells are compared
// with strict equality — a national build's cells and station links are
// held float-exact, and these specs hold the same bar.
var _ = Describe("CellFor", func() {
	DescribeTable("snaps a point to its MERRA-2-anchored centroid",
		func(lat, lon float64, want power.Cell) {
			Expect(power.CellFor(lat, lon)).To(Equal(want))
		},
		// Mérida-ish — typical Yucatán coastal station. The closest
		// centroid on the 0.625°-lon lattice is -89.375 (143 × 0.625),
		// not -89.625.
		Entry("Mérida 20.98, -89.65", 20.98, -89.65,
			power.Cell{ID: "21.0N_89.3750W", Lat: 21.0, Lon: -89.375, Resolution: power.Resolution}),
		// Tacubaya (CDMX) — central plateau.
		Entry("Tacubaya 19.40, -99.20", 19.40, -99.20,
			power.Cell{ID: "19.5N_99.3750W", Lat: 19.5, Lon: -99.375, Resolution: power.Resolution}),
		// Mexicali — far north, near the US border.
		Entry("Mexicali 32.65, -115.47", 32.65, -115.47,
			power.Cell{ID: "32.5N_115.6250W", Lat: 32.5, Lon: -115.625, Resolution: power.Resolution}),
		// Equator / prime meridian sanity — a -0 centroid must not
		// toggle the hemisphere letters.
		Entry("origin 0, 0", 0.0, 0.0,
			power.Cell{ID: "0.0N_0.0000E", Lat: 0.0, Lon: 0.0, Resolution: power.Resolution}),
		// Southern hemisphere with positive longitude exercises both
		// sign branches of CellID.
		Entry("south, positive lon -22.5, 43.125", -22.5, 43.125,
			power.Cell{ID: "22.5S_43.1250E", Lat: -22.5, Lon: 43.125, Resolution: power.Resolution}),
	)

	It("folds nearby stations into one identical cell", func() {
		// ~5 km apart, same MERRA-2 cell — the dedup property the puller
		// relies on to fold the 5,524-station fetch into 609 cells.
		Expect(power.CellFor(20.95, -89.62)).To(Equal(power.CellFor(20.98, -89.65)))
	})
})

var _ = Describe("HaversineKm", func() {
	DescribeTable("matches the mean-sphere reference distances",
		func(lat1, lon1, lat2, lon2, wantKm, tolKm float64) {
			Expect(power.HaversineKm(lat1, lon1, lat2, lon2)).To(BeNumerically("~", wantKm, tolKm))
		},
		Entry("zero distance", 19.0, -99.0, 19.0, -99.0, 0.0, 0.001),
		// 1° lat at the equator: 2πR/360 ≈ 111.195 km.
		Entry("1° latitude at the equator", 0.0, 0.0, 1.0, 0.0, 111.195, 0.5),
		// Mérida (20.98, -89.65) to Tacubaya (19.40, -99.20) —
		// mean-sphere haversine cross-checked externally with the same
		// formula.
		Entry("Mérida to Tacubaya", 20.98, -89.65, 19.40, -99.20, 1011.85, 0.5),
	)
})
