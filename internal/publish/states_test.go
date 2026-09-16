package publish_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/ingest"
	"github.com/bioclimamx/conagua-etl/internal/publish"
)

var _ = Describe("LoadStates", func() {
	It("lists the distinct CONAGUA-conventional states in code order with slug and official name", func() {
		db := openTempDB()
		seedTwoStates(db)
		// An out-of-scope source with a state no conventional station has
		// must not leak into the per-state artifact set.
		upsertStation(db, ingest.StationUpsert{
			Source: ingest.SourceConaguaEMA, ExternalID: "e1", Name: "EMA", State: "ZAC",
		})

		states, err := publish.LoadStates(context.Background(), db)
		Expect(err).NotTo(HaveOccurred())
		Expect(states).To(Equal([]publish.State{
			{Code: "AGS", Slug: "ags", Name: "Aguascalientes"},
			{Code: "YUC", Slug: "yuc", Name: "Yucatán"},
		}))
	})

	It("fails loudly on a NULL state", func() {
		db := openTempDB()
		upsertStation(db, ingest.StationUpsert{
			Source: ingest.SourceConaguaConventional, ExternalID: "9", Name: "Sin estado",
		})
		_, err := publish.LoadStates(context.Background(), db)
		Expect(err).To(MatchError("load states: a conagua_conventional station has a NULL state"))
	})

	It("fails loudly on a code outside the 32 CONAGUA codes", func() {
		db := openTempDB()
		upsertStation(db, ingest.StationUpsert{
			Source: ingest.SourceConaguaConventional, ExternalID: "9", Name: "X", State: "XXX",
		})
		_, err := publish.LoadStates(context.Background(), db)
		Expect(err).To(MatchError(`load states: unknown state code "XXX" in stations (not one of the 32 CONAGUA codes)`))
	})

	It("returns no states for an empty DB", func() {
		states, err := publish.LoadStates(context.Background(), openTempDB())
		Expect(err).NotTo(HaveOccurred())
		Expect(states).To(BeEmpty())
	})
})

var _ = Describe("ResolveStates", func() {
	all := []publish.State{
		{Code: "AGS", Slug: "ags", Name: "Aguascalientes"},
		{Code: "YUC", Slug: "yuc", Name: "Yucatán"},
		{Code: "ZAC", Slug: "zac", Name: "Zacatecas"},
	}

	It("matches case-insensitively, dedupes, and keeps the DB order", func() {
		got, err := publish.ResolveStates(all, []string{"yuc", "AGS", "Yuc"})
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal([]publish.State{all[0], all[1]}))
	})

	It("selects every state when nothing is requested", func() {
		got, err := publish.ResolveStates(all, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(all))
		got[0].Code = "mutated"
		Expect(all[0].Code).To(Equal("AGS"), "the result is a copy")
	})

	It("rejects an unknown code, listing the valid ones", func() {
		_, err := publish.ResolveStates(all, []string{"yuc", "xx"})
		Expect(err).To(MatchError(`unknown state "xx" (valid: AGS, YUC, ZAC)`))
	})
})
