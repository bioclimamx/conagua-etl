package snapshot

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"reflect"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// nationalIndexEnv names the variable holding the path to a national
// snapshot manifest — the real _index.json written for the 2026-06-08
// national pull. It is the characterization fixture for the Progress
// JSON shape: if the types drop, rename, or re-type a field, the
// round-trip below diverges from the original bytes.
const nationalIndexEnv = "SNAPSHOT_NATIONAL_INDEX"

// parseJSON decodes b into a generic tree using json.Number, so integer
// values (notably the uint64 rng_seed, which exceeds float64 precision)
// compare exactly rather than through lossy float64 conversion.
func parseJSON(b []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return v, nil
}

// maxJSONDiffs caps the reported differences so a failure against the
// 11 MB national manifest stays readable.
const maxJSONDiffs = 20

// jsonDiffs appends human-readable differences between two parsed JSON
// trees to out, labelling each with its path. "lost" means a field present
// in the original disappeared after the Progress round-trip; "added" means
// the round-trip emitted a field the original did not have.
func jsonDiffs(path string, orig, rt any, out *[]string) {
	if len(*out) >= maxJSONDiffs {
		return
	}
	switch o := orig.(type) {
	case map[string]any:
		r, ok := rt.(map[string]any)
		if !ok {
			*out = append(*out, fmt.Sprintf("%s: object became %T", path, rt))
			return
		}
		for k, ov := range o {
			rv, ok := r[k]
			if !ok {
				*out = append(*out, fmt.Sprintf("%s/%s: lost in round-trip", path, k))
				continue
			}
			jsonDiffs(path+"/"+k, ov, rv, out)
		}
		for k := range r {
			if _, ok := o[k]; !ok {
				*out = append(*out, fmt.Sprintf("%s/%s: added by round-trip", path, k))
			}
		}
	case []any:
		r, ok := rt.([]any)
		if !ok {
			*out = append(*out, fmt.Sprintf("%s: array became %T", path, rt))
			return
		}
		if len(o) != len(r) {
			*out = append(*out, fmt.Sprintf("%s: array length %d became %d", path, len(o), len(r)))
			return
		}
		for i := range o {
			jsonDiffs(fmt.Sprintf("%s[%d]", path, i), o[i], r[i], out)
		}
	default:
		if !reflect.DeepEqual(orig, rt) {
			*out = append(*out, fmt.Sprintf("%s: %v became %v", path, orig, rt))
		}
	}
}

// The walker guards the parity spec, so its failure modes are themselves
// pinned: a harness that can't see a lost field would let the parity spec
// pass vacuously.
var _ = Describe("jsonDiffs harness", func() {
	diff := func(a, b string) []string {
		ta, err := parseJSON([]byte(a))
		Expect(err).NotTo(HaveOccurred())
		tb, err := parseJSON([]byte(b))
		Expect(err).NotTo(HaveOccurred())
		var out []string
		jsonDiffs("", ta, tb, &out)
		return out
	}

	It("reports nothing for semantically equal documents", func() {
		Expect(diff(`{"a":1,"b":[{"c":"x"}]}`, `{"b":[{"c":"x"}],"a":1}`)).To(BeEmpty())
	})

	It("reports a lost field", func() {
		Expect(diff(`{"a":1,"b":2}`, `{"a":1}`)).To(ConsistOf("/b: lost in round-trip"))
	})

	It("reports an added field", func() {
		Expect(diff(`{"a":1}`, `{"a":1,"b":2}`)).To(ConsistOf("/b: added by round-trip"))
	})

	It("reports a changed value with its path", func() {
		Expect(diff(`{"a":{"b":[1,2]}}`, `{"a":{"b":[1,3]}}`)).To(ConsistOf("/a/b[1]: 2 became 3"))
	})

	It("reports an array length change", func() {
		Expect(diff(`{"a":[1,2]}`, `{"a":[1]}`)).To(ConsistOf("/a: array length 2 became 1"))
	})

	It("distinguishes integers beyond float64 precision", func() {
		Expect(diff(`{"seed":14751612300768752411}`, `{"seed":14751612300768752412}`)).
			To(ConsistOf("/seed: 14751612300768752411 became 14751612300768752412"))
	})
})

var _ = Describe("National index parity", func() {
	It("round-trips the 2026-06-08 national _index.json through the ported types without losing a field", func() {
		path := os.Getenv(nationalIndexEnv)
		if path == "" {
			Skip(nationalIndexEnv + " not set")
		}
		raw, err := os.ReadFile(path)
		if errors.Is(err, fs.ErrNotExist) {
			Skip(nationalIndexEnv + " names an absent file: " + path)
		}
		Expect(err).NotTo(HaveOccurred())

		var p Progress
		Expect(json.Unmarshal(raw, &p)).To(Succeed())

		// Literal anchors from the real national pull — a decode that
		// silently zeroes a field cannot pass these.
		Expect(p.SnapshotDate).To(Equal("2026-06-08"))
		Expect(p.SchemaVersion).To(Equal(ProgressSchemaVersion))
		Expect(p.RNGSeed).To(Equal(uint64(14751612300768752411)))
		Expect(p.Catalog).To(Equal(CatalogSummary{StatesDiscovered: 32, StationsDiscovered: 5524}))
		Expect(p.Counts).To(Equal(Counts{FilesExpected: 22945, FilesFetched: 22945}))
		Expect(p.Stations).To(HaveLen(5524))
		Expect(DeriveCounts(p)).To(Equal(p.Counts), "stored counts must re-derive from station states")

		remarshalled, err := json.Marshal(p)
		Expect(err).NotTo(HaveOccurred())

		origTree, err := parseJSON(raw)
		Expect(err).NotTo(HaveOccurred())
		rtTree, err := parseJSON(remarshalled)
		Expect(err).NotTo(HaveOccurred())

		var diffs []string
		jsonDiffs("", origTree, rtTree, &diffs)
		Expect(diffs).To(BeEmpty(),
			"ported Progress types must round-trip the national index field-for-field")
	})
})
