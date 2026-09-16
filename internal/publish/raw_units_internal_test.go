package publish

// In-package specs for rawUnits, the raw archive's progress units over a
// Path-sorted entry list: one unit per immediate child of the snapshot
// directory — a kind directory however many files it holds, a root
// file — each fired after its last entry's write in list order, only
// once the write succeeded; nil marks nothing and still counts.

import (
	"bytes"
	"errors"
	"io"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/archive"
)

// walkerEntries is a Path-sorted list in the raw walker's shape: the
// two root files, a kind directory of several station files, and two
// kind directories of one; each entry writes its own path.
func walkerEntries() []archive.Entry {
	paths := []string{
		"_index.json", "_progress.json",
		"daily/1001.txt", "daily/31001.txt", "daily/3101.txt",
		"extremes/1001.txt",
		"normals_1991_2020/31001.txt",
	}
	entries := make([]archive.Entry, len(paths))
	for i, p := range paths {
		entries[i] = archive.Entry{Path: p, Write: func(w io.Writer) error {
			_, err := io.WriteString(w, p)
			return err
		}}
	}
	return entries
}

var _ = ginkgo.Describe("rawUnits", func() {
	ginkgo.It("counts one unit per immediate child of the snapshot directory and fires each after its last entry's write, in list order", func() {
		entries := walkerEntries()
		var fired []string
		var buf bytes.Buffer
		var atFire []string
		n := rawUnits(entries, func(label string) {
			fired = append(fired, label)
			atFire = append(atFire, buf.String())
		})
		Expect(n).To(Equal(5))
		for _, e := range entries {
			buf.Reset()
			Expect(e.Write(&buf)).To(Succeed())
			Expect(buf.String()).To(Equal(e.Path), "the write still streams the content")
		}
		Expect(fired).To(Equal([]string{"_index.json", "_progress.json", "daily/", "extremes/", "normals_1991_2020/"}))
		// Each label fired with its last entry's content already written.
		Expect(atFire).To(Equal([]string{
			"_index.json", "_progress.json", "daily/3101.txt", "extremes/1001.txt", "normals_1991_2020/31001.txt",
		}))
	})

	ginkgo.It("counts the units without marking any entry when onUnit is nil", func() {
		entries := walkerEntries()
		Expect(rawUnits(entries, nil)).To(Equal(5))
		var buf bytes.Buffer
		for _, e := range entries {
			buf.Reset()
			Expect(e.Write(&buf)).To(Succeed())
			Expect(buf.String()).To(Equal(e.Path))
		}
	})

	ginkgo.It("does not fire a unit whose last entry failed to write", func() {
		boom := errors.New("disk full")
		entries := []archive.Entry{
			{Path: "daily/1.txt", Write: func(io.Writer) error { return nil }},
			{Path: "daily/2.txt", Write: func(io.Writer) error { return boom }},
		}
		var fired []string
		Expect(rawUnits(entries, func(label string) { fired = append(fired, label) })).To(Equal(1))
		Expect(entries[0].Write(io.Discard)).To(Succeed())
		Expect(entries[1].Write(io.Discard)).To(MatchError(boom))
		Expect(fired).To(BeEmpty())
	})

	ginkgo.It("counts nothing over an empty list", func() {
		Expect(rawUnits(nil, func(string) { ginkgo.Fail("no unit to fire") })).To(BeZero())
	})
})
