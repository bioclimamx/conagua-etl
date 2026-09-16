package schema

import (
	"database/sql"
	"path/filepath"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestSchema(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Schema Suite")
}

// tempDBPath returns a fresh path for a database file inside a
// spec-scoped temp dir. The file does not exist yet.
func tempDBPath() string {
	GinkgoHelper()
	return filepath.Join(GinkgoT().TempDir(), "bioclima.db")
}

// mustOpen opens (creating if needed) a writer DB, failing the spec on
// error.
func mustOpen(path string) *sql.DB {
	GinkgoHelper()
	db, err := Open(path)
	Expect(err).NotTo(HaveOccurred())
	return db
}

// mustClose closes db, failing the spec on error so a dirty close (which
// would leave WAL mid-state on disk) cannot pass silently.
func mustClose(db *sql.DB) {
	GinkgoHelper()
	Expect(db.Close()).To(Succeed())
}
