package conagua

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("StateCode", func() {
	Describe("AllStates", func() {
		It("covers exactly the 32 states with no duplicates", func() {
			Expect(AllStates).To(HaveLen(32))

			seen := make(map[StateCode]bool, len(AllStates))
			for _, c := range AllStates {
				Expect(seen[c]).To(BeFalse(), "duplicate state code %q in AllStates", c)
				seen[c] = true
			}
		})

		It("gives every code a display name and Valid() status", func() {
			for _, c := range AllStates {
				Expect(c.DisplayName()).NotTo(BeEmpty(), "state %q has no display name", c)
				Expect(c.Valid()).To(BeTrue(), "state %q from AllStates is not Valid()", c)
			}
		})

		It("stays in lockstep with stateNames", func() {
			Expect(stateNames).To(HaveLen(len(AllStates)))
		})
	})

	Describe("CatalogURL", func() {
		It("builds the production catalog page URL", func() {
			Expect(Aguascalientes.CatalogURL()).To(Equal(
				"https://smn.conagua.gob.mx/tools/RESOURCES/Normales_Climatologicas/catalogo/cat_ags.html"))
		})

		It("is CatalogURLUnder applied to the production BaseURL", func() {
			for _, c := range AllStates {
				Expect(c.CatalogURL()).To(Equal(c.CatalogURLUnder(BaseURL)))
			}
		})
	})

	Describe("CatalogURLUnder", func() {
		It("re-roots the catalog page under an arbitrary base", func() {
			Expect(Jalisco.CatalogURLUnder("http://127.0.0.1:8912/fixtures/")).To(Equal(
				"http://127.0.0.1:8912/fixtures/catalogo/cat_jal.html"))
		})
	})

	Describe("ParseStateCode", func() {
		It("accepts a known slug", func() {
			c, err := ParseStateCode("jal")
			Expect(err).NotTo(HaveOccurred())
			Expect(c).To(Equal(Jalisco))
		})

		It("rejects an unknown slug, naming it in the error", func() {
			_, err := ParseStateCode("ZZ")
			Expect(err).To(MatchError(ContainSubstring("ZZ")))
		})
	})
})
