package snapshot

import (
	"io"
	"log"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestSnapshot(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Snapshot Suite")
}

// quietLogger discards cache-failure log output so spec output stays clean.
func quietLogger() *log.Logger {
	return log.New(io.Discard, "", 0)
}
