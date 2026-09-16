//go:build unix

package publish_test

// The walker's refusal of a non-regular file that is not a symlink: a
// named pipe inside a kind directory is refused by name and mode rather
// than read (a read would block on it) or skipped (a silently skipped
// path is a lost one). Unix-only: named pipes are made with mkfifo.

import (
	"context"
	"path/filepath"
	"syscall"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/publish"
)

var _ = Describe("RawSnapshotEntries over a named pipe", func() {
	It("refuses the pipe, naming its path and mode, rather than reading or skipping it", func() {
		root := GinkgoT().TempDir()
		seedWalkerSnapshot(root)
		pipe := filepath.Join(publish.RawSnapshotDir(root, rawDate), "daily", "pipe")
		Expect(syscall.Mkfifo(pipe, 0o600)).To(Succeed())

		entries, err := publish.RawSnapshotEntries(context.Background(), root, rawDate)
		Expect(entries).To(BeNil())
		Expect(err).To(MatchError(ContainSubstring("daily/pipe is of mode p")))
		Expect(err).To(MatchError(ContainSubstring(", not a regular file")))
		Expect(err).To(MatchError(HavePrefix("walk snapshot directory " + publish.RawSnapshotDir(root, rawDate) + ": ")))
	})
})
