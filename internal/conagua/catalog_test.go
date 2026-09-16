package conagua

import (
	"bytes"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// wantAgsStations is the full expected parse of testdata/cat_ags.html
// (a golden fixture), in document order, every field included. Values
// are a recorded reference parse of the same fixture, so this table pins
// the parser's output field for field.
var wantAgsStations = []Station{
	{
		State: Aguascalientes, ID: "01001", Name: "Aguascalientes (Obs)",
		Municipality: "Aguascalientes", Status: StatusOperating,
		Files: map[Kind]FileEntry{
			KindDaily:            {URL: "https://smn.conagua.gob.mx/tools/RESOURCES/Normales_Climatologicas/Diarios/ags/dia01001.txt"},
			KindMonthly:          {URL: "https://smn.conagua.gob.mx/tools/RESOURCES/Normales_Climatologicas/Mensuales/ags/mes01001.txt"},
			KindExtremes:         {URL: "https://smn.conagua.gob.mx/tools/RESOURCES/Normales_Climatologicas/Med-Extr/ags/medex01001.txt"},
			KindNormals1961_1990: {URL: "https://smn.conagua.gob.mx/tools/RESOURCES/Normales_Climatologicas/Normales6190/ags/nor6190_01001.txt"},
			KindNormals1971_2000: {URL: "https://smn.conagua.gob.mx/tools/RESOURCES/Normales_Climatologicas/Normales7100/ags/nor7100_01001.txt"},
			KindNormals1981_2010: {URL: "https://smn.conagua.gob.mx/tools/RESOURCES/Normales_Climatologicas/Normales8110/ags/nor8110_01001.txt"},
			KindNormals1991_2020: {URL: "https://smn.conagua.gob.mx/tools/RESOURCES/Normales_Climatologicas/Normales9120/ags/nor9120_01001.txt"},
		},
	},
	{
		State: Aguascalientes, ID: "01003", Name: "Calvillo (Smn)",
		Municipality: "Calvillo", Status: StatusSuspended,
		Files: map[Kind]FileEntry{
			KindDaily:            {URL: "https://smn.conagua.gob.mx/tools/RESOURCES/Normales_Climatologicas/Diarios/ags/dia01003.txt"},
			KindMonthly:          {URL: "https://smn.conagua.gob.mx/tools/RESOURCES/Normales_Climatologicas/Mensuales/ags/mes01003.txt"},
			KindExtremes:         {URL: "https://smn.conagua.gob.mx/tools/RESOURCES/Normales_Climatologicas/Med-Extr/ags/medex01003.txt"},
			KindNormals1961_1990: {URL: "https://smn.conagua.gob.mx/tools/RESOURCES/Normales_Climatologicas/Normales6190/ags/nor6190_01003.txt"},
		},
	},
	{
		State: Aguascalientes, ID: "01004", Name: "Cañada Honda",
		Municipality: "Aguascalientes", Status: StatusOperating,
		Files: map[Kind]FileEntry{
			KindDaily:            {URL: "https://smn.conagua.gob.mx/tools/RESOURCES/Normales_Climatologicas/Diarios/ags/dia01004.txt"},
			KindMonthly:          {URL: "https://smn.conagua.gob.mx/tools/RESOURCES/Normales_Climatologicas/Mensuales/ags/mes01004.txt"},
			KindExtremes:         {URL: "https://smn.conagua.gob.mx/tools/RESOURCES/Normales_Climatologicas/Med-Extr/ags/medex01004.txt"},
			KindNormals1971_2000: {URL: "https://smn.conagua.gob.mx/tools/RESOURCES/Normales_Climatologicas/Normales7100/ags/nor7100_01004.txt"},
			KindNormals1981_2010: {URL: "https://smn.conagua.gob.mx/tools/RESOURCES/Normales_Climatologicas/Normales8110/ags/nor8110_01004.txt"},
			KindNormals1991_2020: {URL: "https://smn.conagua.gob.mx/tools/RESOURCES/Normales_Climatologicas/Normales9120/ags/nor9120_01004.txt"},
		},
	},
	{
		State: Aguascalientes, ID: "01025", Name: "San Francisco De Los Romo (Smn)",
		Municipality: "San Francisco De Los Romo", Status: StatusSuspended,
		Files: map[Kind]FileEntry{
			KindDaily:    {URL: "https://smn.conagua.gob.mx/tools/RESOURCES/Normales_Climatologicas/Diarios/ags/dia01025.txt"},
			KindMonthly:  {URL: "https://smn.conagua.gob.mx/tools/RESOURCES/Normales_Climatologicas/Mensuales/ags/mes01025.txt"},
			KindExtremes: {URL: "https://smn.conagua.gob.mx/tools/RESOURCES/Normales_Climatologicas/Med-Extr/ags/medex01025.txt"},
		},
	},
	{
		State: Aguascalientes, ID: "01097", Name: "Aguascalientes Ii",
		Municipality: "Aguascalientes", Status: StatusOperating,
		Files: map[Kind]FileEntry{
			KindDaily:            {URL: "https://smn.conagua.gob.mx/tools/RESOURCES/Normales_Climatologicas/Diarios/ags/dia01097.txt"},
			KindMonthly:          {URL: "https://smn.conagua.gob.mx/tools/RESOURCES/Normales_Climatologicas/Mensuales/ags/mes01097.txt"},
			KindExtremes:         {URL: "https://smn.conagua.gob.mx/tools/RESOURCES/Normales_Climatologicas/Med-Extr/ags/medex01097.txt"},
			KindNormals1991_2020: {URL: "https://smn.conagua.gob.mx/tools/RESOURCES/Normales_Climatologicas/Normales9120/ags/nor9120_01097.txt"},
		},
	},
}

var _ = Describe("ParseCatalog", func() {
	Context("with the Aguascalientes golden fixture", func() {
		var stations []Station

		BeforeEach(func() {
			var err error
			stations, err = ParseCatalog(
				Aguascalientes, Aguascalientes.CatalogURL(),
				bytes.NewReader(readFixture("cat_ags.html")))
			Expect(err).NotTo(HaveOccurred())
		})

		It("round-trips every station field, in document order", func() {
			Expect(stations).To(HaveLen(len(wantAgsStations)))
			for i, want := range wantAgsStations {
				Expect(stations[i]).To(Equal(want), "station %s", want.ID)
			}
		})

		// The per-station shapes worth naming, restated on
		// top of the full round-trip so a regression names the scenario:
		// 01001 carries all 7 kinds; 01003 is suspended with only the
		// 1961-1990 normals; 01004 keeps its non-ASCII ñ and lacks only
		// 1961-1990; 01025 is historic-only (no normals at all); 01097 has
		// only the 1991-2020 normals.
		It("classifies the full 7-kind complement on 01001", func() {
			Expect(stations[0].Files).To(HaveLen(len(AllKinds)))
			for _, k := range AllKinds {
				Expect(stations[0].Files).To(HaveKey(k))
			}
		})
	})

	It("rejects an invalid state code", func() {
		_, err := ParseCatalog(StateCode("nope"), BaseURL, bytes.NewReader(nil))
		Expect(err).To(MatchError(ContainSubstring("invalid state code")))
	})
})

var _ = DescribeTable("parseStatus",
	func(input string, want Status) {
		Expect(parseStatus(input)).To(Equal(want))
	},
	Entry("Operando", "Operando", StatusOperating),
	Entry("operando", "operando", StatusOperating),
	Entry("OPERANDO", "OPERANDO", StatusOperating),
	Entry("Suspendida", "Suspendida", StatusSuspended),
	Entry("Suspendido", "Suspendido", StatusSuspended),
	Entry("empty", "", StatusUnknown),
	Entry("unrecognized", "unknown", StatusUnknown),
)

var _ = DescribeTable("ShortID",
	func(catalogID, want string) {
		Expect(ShortID(catalogID)).To(Equal(want))
	},
	Entry("5-digit URL form", "01001", "1001"),
	Entry("already short", "1001", "1001"),
	Entry("all zeros", "00000", "0"),
	Entry("non-numeric passthrough", "AB123", "AB123"),
	Entry("empty", "", ""),
)

var _ = Describe("NormalsKinds", func() {
	It("lists the four ingestable normals kinds, oldest first", func() {
		Expect(NormalsKinds).To(Equal([]Kind{
			KindNormals1961_1990, KindNormals1971_2000,
			KindNormals1981_2010, KindNormals1991_2020,
		}))
	})

	It("maps every entry to a period via PeriodForKind", func() {
		for _, k := range NormalsKinds {
			_, ok := PeriodForKind(k)
			Expect(ok).To(BeTrue(), "kind %q", k)
		}
	})

	It("excludes the non-normals kinds", func() {
		Expect(NormalsKinds).NotTo(ContainElements(KindDaily, KindMonthly, KindExtremes))
	})
})
