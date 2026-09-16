package fetcher

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/conagua"
)

func TestFetcher(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Fetcher Suite")
}

// snapDate is the fixed snapshot date every integration spec writes under.
const snapDate = "2026-07-17"

// sha256Hex computes the digest the sink is expected to report, independently
// of the sink's own incremental hashing.
func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// testStation builds a catalog-shaped station. The fetcher itself only reads
// ID (for the sink address); the remaining fields ride along so Result.Task
// equality proves the whole struct is carried through untouched.
func testStation(id string) conagua.Station {
	return conagua.Station{
		State:        conagua.Aguascalientes,
		ID:           id,
		Name:         "STATION " + id,
		Municipality: "PABELLON DE ARTEAGA",
		Status:       conagua.StatusOperating,
	}
}

// probeWaits advances l by n ticks via NextWait — the non-sleeping probe —
// and returns the full WaitInfo sequence for exact comparison.
func probeWaits(l *Limiter, n int) []WaitInfo {
	out := make([]WaitInfo, n)
	for i := range out {
		out[i] = l.NextWait()
	}
	return out
}
