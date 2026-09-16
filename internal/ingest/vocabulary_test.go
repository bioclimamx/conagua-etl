package ingest_test

import (
	"os"
	"path/filepath"
	"regexp"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/ingest"
)

// The Source enum's Go owner is ingest; schema.sql's CHECK literal
// cannot reference Go, so this spec is the lockstep tripwire: the DDL's
// stations.source CHECK
// set must equal the six ingest.Source constants exactly.
var _ = Describe("DDL lockstep with the Source vocabulary", func() {
	It("enumerates exactly the six ingest.Source values in the stations.source CHECK", func() {
		ddl, err := os.ReadFile(filepath.Join("..", "schema", "schema.sql"))
		Expect(err).NotTo(HaveOccurred())

		checkRe := regexp.MustCompile(`(?s)CHECK \(source IN \((.*?)\)\)`)
		m := checkRe.FindSubmatch(ddl)
		Expect(m).NotTo(BeNil(), "stations must carry a source CHECK")

		var ddlSources []string
		for _, lit := range regexp.MustCompile(`'([^']+)'`).FindAllSubmatch(m[1], -1) {
			ddlSources = append(ddlSources, string(lit[1]))
		}

		Expect(ddlSources).To(ConsistOf(
			string(ingest.SourceConaguaConventional),
			string(ingest.SourceConaguaEMA),
			string(ingest.SourceINIFAP),
			string(ingest.SourceNASAPower),
			string(ingest.SourceERA5),
			string(ingest.SourceMeta),
		))
	})
})
