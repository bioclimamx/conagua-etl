package publish

// In-package specs for the scope: the predicate and its arguments are
// one definition — a state binds (state, source), the national scope
// (source) alone, the source predicate never dropped — and every
// whole-scope query at national scope is the state query with the state
// predicate swapped and nothing else, so the national per-table files
// are the state files' queries by construction, never a second SQL
// text.

import (
	"strings"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = ginkgo.Describe("scope", func() {
	state := stateScope(State{Code: "YUC", Slug: "yuc", Name: "Yucatán"})

	ginkgo.It("binds (state, source) for a state and (source) nationally, never dropping the source predicate", func() {
		Expect(state.predicate()).To(Equal("WHERE s.state = ? AND s.source = ?"))
		Expect(state.args()).To(Equal([]any{"YUC", "conagua_conventional"}))
		Expect(state.label()).To(Equal("YUC"))
		Expect(nationalScope.predicate()).To(Equal("WHERE s.source = ?"))
		Expect(nationalScope.args()).To(Equal([]any{"conagua_conventional"}))
		Expect(nationalScope.label()).To(Equal("all states"))
	})

	ginkgo.It("writes each whole-scope query once: the national text is the state text with the predicate swapped, its placeholders exactly the arguments", func() {
		queries := map[string]func(scope) string{
			"conagua/stations":               stationsSQL,
			"conagua/monthly_normals":        normalsSQL,
			"conagua/monthly_normals_extras": extrasSQL,
			"nasa_power/cells":               cellsSQL,
			"nasa_power/station_cell_map":    stationCellMapSQL,
			"nasa_power/monthly":             powerMonthlySQL,
			"combined/combined_monthly":      combinedMonthlySQL,
			"the station list":               stationListSQL,
			"the cell set":                   cellSetSQL,
		}
		for name, q := range queries {
			Expect(strings.Count(q(state), state.predicate())).To(Equal(1), name)
			Expect(q(nationalScope)).To(Equal(strings.Replace(q(state), state.predicate(), nationalScope.predicate(), 1)), name)
			Expect(strings.Count(q(state), "?")).To(Equal(len(state.args())), name)
			Expect(strings.Count(q(nationalScope), "?")).To(Equal(len(nationalScope.args())), name)
		}
	})
})
