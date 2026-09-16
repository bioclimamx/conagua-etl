package conagua

import (
	"bufio"
	"bytes"
	"io"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// parseHeaderFixture runs ParseHeader over a header/ fixture and returns
// the reader too, so specs can verify where the parse left it positioned.
func parseHeaderFixture(name string) (h Header, warnings []Warning, br *bufio.Reader, err error) {
	GinkgoHelper()
	br = bufio.NewReader(bytes.NewReader(readFixture("header", name)))
	h, warnings, err = ParseHeader(br)
	return h, warnings, br, err
}

var _ = Describe("ParseHeader", func() {
	Context("with a real daily file (station 01001)", func() {
		It("round-trips every header field", func() {
			h, warnings, _, err := parseHeaderFixture("real_daily_01001.txt")
			Expect(err).NotTo(HaveOccurred())
			Expect(warnings).To(BeEmpty())
			Expect(h).To(Equal(Header{
				ExternalID:   "1001",
				Name:         "AGUASCALIENTES (OBS)",
				State:        "AGUASCALIENTES",
				Municipality: "AGUASCALIENTES",
				Status:       StatusOperating,
				CVEOMM:       "76571",
				Lat:          fp(21.85027778),
				Lon:          fp(-102.2908333),
				AltitudeM:    fp(1890.8),
			}))
		})

		It("leaves the reader positioned at the data section", func() {
			_, _, br, err := parseHeaderFixture("real_daily_01001.txt")
			Expect(err).NotTo(HaveOccurred())

			// CONAGUA leaves a few blank lines after the header; the first
			// non-blank line must be the FECHA column-header row, proving
			// ParseHeader stopped exactly at the header/data boundary
			// (neither early, inside the header, nor late, eating data).
			for {
				line, readErr := br.ReadString('\n')
				Expect(readErr).NotTo(HaveOccurred())
				if strings.TrimSpace(line) == "" {
					continue
				}
				Expect(line).To(Equal("FECHA\t\tPRECIP\tEVAP\tTMAX\tTMIN\n"))
				break
			}
		})
	})

	Context("with a malformed LATITUD value", func() {
		It("warns, nils the field, and keeps parsing the rest", func() {
			h, warnings, _, err := parseHeaderFixture("malformed_lat.txt")
			Expect(err).NotTo(HaveOccurred())
			Expect(h).To(Equal(Header{
				ExternalID:   "1001",
				Name:         "TESTVILLE",
				State:        "NUEVO LEÓN",
				Municipality: "MONTERREY",
				Status:       StatusOperating,
				CVEOMM:       "76571",
				Lat:          nil,
				Lon:          fp(-102.2908333),
				AltitudeM:    fp(1890.8),
			}))
			Expect(warnings).To(Equal([]Warning{{
				Line:    9,
				Message: `malformed LATITUD "abc °": strconv.ParseFloat: parsing "abc": invalid syntax`,
			}}))
		})
	})

	Context("with a sparse header (missing fields, NULO coordinates)", func() {
		It("leaves absent fields zero-valued and NULO coordinates nil, without warnings", func() {
			h, warnings, _, err := parseHeaderFixture("missing_fields.txt")
			Expect(err).NotTo(HaveOccurred())
			Expect(warnings).To(BeEmpty())
			Expect(h).To(Equal(Header{
				ExternalID: "9999",
				Name:       "SPARSELAND",
				Status:     StatusSuspended,
			}))
		})
	})

	Context("with a header-less file", func() {
		It("returns an error", func() {
			_, _, _, err := parseHeaderFixture("empty.txt")
			Expect(err).To(MatchError(ContainSubstring("no station header found")))
		})
	})

	Context("with a UTF-8 BOM prefix", func() {
		It("strips the BOM instead of pushing it into the first field", func() {
			h, warnings, _, err := parseHeaderFixture("bom_prefix.txt")
			Expect(err).NotTo(HaveOccurred())
			Expect(warnings).To(BeEmpty())
			Expect(h).To(Equal(Header{
				ExternalID:   "1001",
				Name:         "BOMVILLE",
				State:        "JALISCO",
				Municipality: "GUADALAJARA",
				Status:       StatusOperating,
				CVEOMM:       "77777",
				Lat:          fp(20.6667),
				Lon:          fp(-103.35),
				AltitudeM:    fp(1566),
			}))
		})
	})

	Context("with mojibake-escaped header keys", func() {
		// Pins the fix for a real CONAGUA quirk: some station files escape
		// non-ASCII header characters as literal "<c3><93>" (raw UTF-8
		// bytes) or "<U+00D1>" (codepoints) instead of clean UTF-8 — both
		// forms appeared in station 01004's daily file during the
		// 2026-04-23 national ingest. Without the decoder, the ESTACIÓN key
		// wouldn't match, ExternalID would end up empty, and ingest would
		// reject the file.
		It("decodes both escape forms and matches the header keys", func() {
			input := "" +
				"COMISI<c3><93>N NACIONAL DEL AGUA\n" +
				"\n" +
				" ESTACI<c3><93>N  : 1004 \n" +
				" NOMBRE    : CA<U+00D1>ADA HONDA \n" +
				" ESTADO    : AGUASCALIENTES \n" +
				" MUNICIPIO : AGUASCALIENTES \n" +
				" SITUACI<c3><93>N : OPERANDO \n" +
				"\n"
			h, warnings, err := ParseHeader(bufio.NewReader(strings.NewReader(input)))
			Expect(err).NotTo(HaveOccurred())
			Expect(warnings).To(BeEmpty())
			Expect(h).To(Equal(Header{
				ExternalID:   "1004",
				Name:         "CAÑADA HONDA",
				State:        "AGUASCALIENTES",
				Municipality: "AGUASCALIENTES",
				Status:       StatusOperating,
			}))
		})
	})

	Context("with a value CONAGUA wrapped onto its own line (station 25171)", func() {
		// Characterization fixture: the first 29 lines of
		// snapshots/conagua-raw/2026-08-30/daily/25171.txt, byte for
		// byte. CONAGUA hard-wraps that station's LONGITUD before its
		// degree sign, stranding a bare "°" on line 20 — and the orphan
		// used to end the header parse, so ALTITUD on line 21 was never
		// read. 25171 was the only station in the national table whose
		// altitude_m was NULL while CONAGUA published a value, and the
		// drop wrote no warning. Both halves are pinned here.
		It("reads the fields below the wrap and reports the wrap", func() {
			h, warnings, _, err := parseHeaderFixture("real_daily_25171_wrapped.txt")
			Expect(err).NotTo(HaveOccurred())
			Expect(h).To(Equal(Header{
				ExternalID:   "25171",
				Name:         "TOBOLOTO",
				State:        "SINALOA",
				Municipality: "NAVOLATO",
				Status:       StatusOperating,
				CVEOMM:       "",
				Lat:          fp(24.76555556),
				Lon:          fp(-107.722419),
				AltitudeM:    fp(16),
			}))
			Expect(warnings).To(Equal([]Warning{{
				Line:    20,
				Message: `wrapped LONGITUD value continued on this line; rejoined as "-107.722419°"`,
			}}))
		})

		It("still ends the header exactly at the blank separator", func() {
			_, _, br, err := parseHeaderFixture("real_daily_25171_wrapped.txt")
			Expect(err).NotTo(HaveOccurred())

			// Recovering the wrap must not move the header/data boundary:
			// the first non-blank line left to the caller is still the
			// FECHA column-header row.
			for {
				line, readErr := br.ReadString('\n')
				Expect(readErr).NotTo(HaveOccurred())
				if strings.TrimSpace(line) == "" {
					continue
				}
				Expect(line).To(Equal("FECHA\t\tPRECIP\tEVAP\tTMAX\tTMIN\n"))
				break
			}
		})
	})

	Context("with an uninterpretable line inside the header block", func() {
		It("ends the header there, reports the line, and leaves the rest to the caller", func() {
			input := "" +
				" ESTACIÓN  : 1001 \n" +
				" NOMBRE    : TESTVILLE \n" +
				" LATITUD   : 20.6667 ° \n" +
				"???\n" +
				" ALTITUD   : 1566 msnm \n" +
				"\n"
			br := bufio.NewReader(strings.NewReader(input))
			h, warnings, err := ParseHeader(br)
			Expect(err).NotTo(HaveOccurred())
			Expect(h).To(Equal(Header{
				ExternalID: "1001",
				Name:       "TESTVILLE",
				Lat:        fp(20.6667),
			}))
			Expect(warnings).To(Equal([]Warning{{
				Line:    4,
				Message: `header block ended at unrecognized line "???" (line skipped)`,
			}}))

			// Termination semantics are unchanged: the parser stops dead
			// at the line it cannot read rather than scanning on for the
			// ALTITUD below it, so everything after stays with the caller.
			rest, readErr := io.ReadAll(br)
			Expect(readErr).NotTo(HaveOccurred())
			Expect(string(rest)).To(Equal(" ALTITUD   : 1566 msnm \n\n"))
		})

		It("truncates a long line in the warning it reports", func() {
			long := strings.Repeat("x", 100)
			input := " ESTACIÓN  : 1001 \n" + long + "\n"
			_, warnings, err := ParseHeader(bufio.NewReader(strings.NewReader(input)))
			Expect(err).NotTo(HaveOccurred())
			Expect(warnings).To(Equal([]Warning{{
				Line:    2,
				Message: `header block ended at unrecognized line "` + strings.Repeat("x", 60) + `…" (line skipped)`,
			}}))
		})
	})

	Context("with a stranded fragment that is not a wrapped numeric value", func() {
		// The recovery is deliberately narrow: only a known unit tail,
		// only onto the numeric field directly above it. These pin the
		// two ways a looser rule would corrupt the header — absorbing a
		// data row, or appending junk to a text field.
		It("does not rejoin a unit tail onto a text field", func() {
			input := "" +
				" ESTACIÓN  : 1001 \n" +
				" NOMBRE    : TOBOLOTO \n" +
				" ° \n"
			h, warnings, err := ParseHeader(bufio.NewReader(strings.NewReader(input)))
			Expect(err).NotTo(HaveOccurred())
			Expect(h).To(Equal(Header{ExternalID: "1001", Name: "TOBOLOTO"}))
			Expect(warnings).To(Equal([]Warning{{
				Line:    3,
				Message: `header block ended at unrecognized line "°" (line skipped)`,
			}}))
		})

		It("does not absorb a bare numeric row into the coordinate above it", func() {
			input := "" +
				" ESTACIÓN  : 1001 \n" +
				" LONGITUD  : -103.35 \n" +
				"1978\n"
			h, warnings, err := ParseHeader(bufio.NewReader(strings.NewReader(input)))
			Expect(err).NotTo(HaveOccurred())
			Expect(h).To(Equal(Header{ExternalID: "1001", Lon: fp(-103.35)}))
			Expect(warnings).To(Equal([]Warning{{
				Line:    3,
				Message: `header block ended at unrecognized line "1978" (line skipped)`,
			}}))
		})
	})
})

var _ = DescribeTable("rejoinWrappedValue",
	func(key, val, frag, wantJoined string, wantOK bool) {
		joined, ok := rejoinWrappedValue(key, val, frag)
		Expect(ok).To(Equal(wantOK))
		Expect(joined).To(Equal(wantJoined))
	},
	Entry("the real 25171 wrap: LONGITUD stranded from its degree sign",
		"LONGITUD", "-107.722419", " ° ", "-107.722419°", true),
	Entry("ALTITUD stranded from its unit",
		"ALTITUD", "16", "msnm", "16msnm", true),
	Entry("uppercase unit tail",
		"ALTITUD", "16", "MSNM", "16MSNM", true),
	Entry("a value that already carries its unit rejects a second one",
		"LONGITUD", "-107.722419 °", "°", "", false),
	Entry("a fragment that is not a known unit tail",
		"LATITUD", "20.6667", "1978", "", false),
	Entry("a text field never rejoins",
		"NOMBRE", "TOBOLOTO", "°", "", false),
	Entry("no field precedes the fragment",
		"", "", "°", "", false),
)

var _ = Describe("decodeConaguaMojibake", func() {
	It("is a no-op on clean UTF-8 input", func() {
		clean := " ESTACIÓN  : 1001 — NOMBRE : AGUASCALIENTES (DGE) "
		Expect(decodeConaguaMojibake(clean)).To(Equal(clean))
	})
})

var _ = DescribeTable("isMissingToken",
	func(s string, want bool) {
		Expect(isMissingToken(s)).To(Equal(want))
	},
	Entry("empty", "", true),
	Entry("whitespace only", "   ", true),
	Entry("NULO", "NULO", true),
	Entry("nulo lowercase", "nulo", true),
	Entry("-999 sentinel", "-999", true),
	Entry("-9999 sentinel", "-9999", true),
	Entry("zero is a value", "0", false),
	Entry("ordinary number", "21.5", false),
	Entry("arbitrary text is not missing (it must warn downstream)", "abc", false),
)
