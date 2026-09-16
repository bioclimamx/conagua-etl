// Comparator completeness specs, in-package to reach the unexported
// diff* functions. (The suite bootstrap and the behavioral specs live in
// parity_test; both packages compile into the same test binary and run
// under the one RunSpecs. ginkgo is imported by name, not dot-imported,
// because its Report would collide with parity.Report — the same
// collision that forced the suite bootstrap external.)
//
// The gate: each diff function must cover every field of its conagua
// struct, with the row key (Date / Month) as the only exclusion. The
// specs generate a struct pair differing in every field via reflection
// and assert the recorded revision Fields match the struct's field
// names exactly — so a field added upstream (e.g. RH returning to
// DailyRow) fails here until the comparator itemizes it, instead of
// silently escaping the parity gate.
package parity

import (
	"fmt"
	"reflect"
	"slices"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/conagua"
)

// setSeeded writes a deterministic seed-derived value into v. Pointer
// fields are allocated and their pointees seeded, never left nil — a nil
// on both sides would compare equal and let an uncovered field hide.
func setSeeded(v reflect.Value, seed int64) {
	switch v.Kind() {
	case reflect.Pointer:
		p := reflect.New(v.Type().Elem())
		setSeeded(p.Elem(), seed)
		v.Set(p)
	case reflect.String:
		v.SetString(fmt.Sprintf("v%d", seed))
	case reflect.Int:
		v.SetInt(seed)
	case reflect.Float64:
		v.SetFloat(float64(seed))
	default:
		ginkgo.Fail(fmt.Sprintf("field kind %s not supported — extend setSeeded", v.Kind()))
	}
}

// differingPair builds two values of struct type T that differ in every
// field except the named keys, which carry the same value on both sides.
func differingPair[T any](keys ...string) (base, updated T) {
	bv := reflect.ValueOf(&base).Elem()
	uv := reflect.ValueOf(&updated).Elem()
	for i := range bv.NumField() {
		baseSeed := int64(2*i + 1)
		updatedSeed := baseSeed + 1
		if slices.Contains(keys, bv.Type().Field(i).Name) {
			updatedSeed = baseSeed
		}
		setSeeded(bv.Field(i), baseSeed)
		setSeeded(uv.Field(i), updatedSeed)
	}
	return base, updated
}

// wantFields lists prefix+name for every field of struct type T except
// the named keys — the exact revision Fields a complete diff must emit.
func wantFields[T any](prefix string, keys ...string) []string {
	t := reflect.TypeFor[T]()
	fields := make([]string, 0, t.NumField())
	for i := range t.NumField() {
		if name := t.Field(i).Name; !slices.Contains(keys, name) {
			fields = append(fields, prefix+name)
		}
	}
	return fields
}

// recordedFields extracts the Field of every recorded revision.
func recordedFields(revs []Revision) []string {
	fields := make([]string, len(revs))
	for i, rev := range revs {
		fields[i] = rev.Field
	}
	return fields
}

var _ = ginkgo.Describe("comparator completeness", func() {
	ginkgo.It("covers every conagua.Header field", func() {
		base, updated := differingPair[conagua.Header]()
		revs := diffHeader("01001", base, updated)
		Expect(recordedFields(revs)).To(ConsistOf(wantFields[conagua.Header]("Header.")))
	})

	ginkgo.It("covers every conagua.DailyRow field except the Date key", func() {
		base, updated := differingPair[conagua.DailyRow]("Date")
		c := revCollector{station: "01001", date: base.Date}
		diffDailyRow(&c, base, updated)
		Expect(recordedFields(c.revs)).To(ConsistOf(wantFields[conagua.DailyRow]("", "Date")))
	})

	ginkgo.It("covers every conagua.MonthlyNormalsRow field except the Month key", func() {
		base, updated := differingPair[conagua.MonthlyNormalsRow]("Month")
		c := revCollector{station: "01001", month: base.Month}
		diffNormalsRow(&c, base, updated)
		Expect(recordedFields(c.revs)).To(ConsistOf(wantFields[conagua.MonthlyNormalsRow]("", "Month")))
	})

	ginkgo.It("covers every conagua.MonthlyNormalsExtrasRow field except the Month key", func() {
		base, updated := differingPair[conagua.MonthlyNormalsExtrasRow]("Month")
		c := revCollector{station: "01001", month: base.Month}
		diffExtrasRow(&c, base, updated)
		Expect(recordedFields(c.revs)).To(ConsistOf(wantFields[conagua.MonthlyNormalsExtrasRow]("", "Month")))
	})
})
