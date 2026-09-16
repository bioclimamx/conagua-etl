package cmd

// Direct specs for the mirrorLog renderer, pinning all four result-line
// shapes (OK / SKIP / ERR-mismatch / ERR) as literal strings — layout
// drift in any shape fails here even where the e2e layer cannot reach it
// (--sink local never produces an OK line: verify mode only skips).

import (
	"bytes"
	"errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/conagua"
)

var _ = Describe("mirrorLog", func() {
	var (
		out, errBuf bytes.Buffer
		r           *mirrorLog
	)

	entry := func(station string, kind conagua.Kind) mirrorEntry {
		return mirrorEntry{State: conagua.Aguascalientes, StationID: station, Kind: kind}
	}

	BeforeEach(func() {
		out.Reset()
		errBuf.Reset()
		r = newMirrorLog(&out, &errBuf)
	})

	It("routes the plan summary to the meta writer only", func() {
		r.plan(4, 3)
		Expect(errBuf.String()).To(Equal("planned 4 fetched files across 3 stations\n\n"))
		Expect(out.String()).To(BeEmpty())
	})

	It("renders all four line shapes with a pinned layout and tallies them", func() {
		r.plan(4, 3)
		errBuf.Reset() // the result lines below must land on out alone

		r.result(entry("01001", conagua.KindDaily),
			mirrorStatusFetched, 1016, nil)
		r.result(entry("01001", conagua.KindNormals1991_2020),
			mirrorStatusVerified, 23, nil)
		r.result(entry("01003", conagua.KindDaily),
			mirrorStatusMismatch, 40, errors.New(
				"local sha256=aaaaaaaaaaaa… != index sha256=bbbbbbbbbbbb… (40 B local, 998 B expected) — kept, not overwritten"))
		r.result(entry("01097", conagua.KindNormals1991_2020),
			mirrorStatusError, 0, errors.New("sink get: connection reset"))

		Expect(out.String()).To(Equal(
			"[    1/    4] ags/01001 daily             OK      1016 B\n" +
				"[    2/    4] ags/01001 normals_1991_2020 SKIP      23 B  (verified)\n" +
				"[    3/    4] ags/01003 daily             ERR  local sha256=aaaaaaaaaaaa… != index sha256=bbbbbbbbbbbb… (40 B local, 998 B expected) — kept, not overwritten\n" +
				"[    4/    4] ags/01097 normals_1991_2020 ERR  sink get: connection reset\n"))
		Expect(errBuf.String()).To(BeEmpty())

		mirrored, skipped, errored, mismatches := r.totals()
		Expect(mirrored).To(Equal(1))
		Expect(skipped).To(Equal(1))
		Expect(errored).To(Equal(2), "a mismatch counts toward the exit-code errors")
		Expect(mismatches).To(Equal(1), "but only the mismatch earns the kept-untouched note")
	})
})
