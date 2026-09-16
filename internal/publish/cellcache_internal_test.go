package publish

// In-package spec for the per-cell memo: a cell's profile reads run once
// per cache, and a later station on the same cell is served from memory.

import (
	"context"
	"path/filepath"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/schema"
)

var _ = ginkgo.Describe("cellCache", func() {
	ginkgo.It("loads a cell once and serves the same profile from memory afterwards, even with the DB gone", func() {
		db, err := schema.Open(filepath.Join(ginkgo.GinkgoT().TempDir(), "cache.db"))
		Expect(err).NotTo(HaveOccurred())
		_, err = db.Exec(`INSERT INTO nasa_power_grid_cells (cell_id, lat, lon, grid_resolution) VALUES ('21.5N_89.3750W', 21.5, -89.375, '0.5x0.625')`)
		Expect(err).NotTo(HaveOccurred())

		cache := newCellCache()
		first, err := cache.get(context.Background(), db, "21.5N_89.3750W")
		Expect(err).NotTo(HaveOccurred())
		Expect(first.monthly).To(HaveLen(len(normalsPeriods)))
		for _, p := range normalsPeriods {
			Expect(first.monthly).To(HaveKeyWithValue(p, BeNil()))
		}
		Expect(first.reanalysis).To(BeNil())
		Expect(cache.cells).To(HaveLen(1))

		Expect(db.Close()).To(Succeed())
		again, err := cache.get(context.Background(), db, "21.5N_89.3750W")
		Expect(err).NotTo(HaveOccurred())
		Expect(again).To(BeIdenticalTo(first))
		Expect(cache.cells).To(HaveLen(1))
	})

	ginkgo.It("does not memoize a failed load", func() {
		db, err := schema.Open(filepath.Join(ginkgo.GinkgoT().TempDir(), "cache.db"))
		Expect(err).NotTo(HaveOccurred())
		Expect(db.Close()).To(Succeed())
		cache := newCellCache()
		_, err = cache.get(context.Background(), db, "21.5N_89.3750W")
		Expect(err).To(HaveOccurred())
		Expect(cache.cells).To(BeEmpty())
	})
})
