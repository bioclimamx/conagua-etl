package parity_test

// Synthetic validate-DB fixtures for the validate comparator specs.
// Both sides are created through schema.Open — the real DDL, the real
// driver — and seeded through plain SQL with explicit surrogate ids, so
// every spec exercises the exact SELECT the production gate runs. The
// two sides always get *different* surrogate station ids: alignment on
// the station's (source, external_id) natural key, never on the
// surrogate, is the contract under test. The seed carries one row per
// reference rule shape, a NULL station row (orphan-runs), a duplicated
// tuple (multiset counting), and one ingest-written warning the
// comparator must ignore.

import (
	"context"
	"database/sql"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/parity"
)

const (
	validateExtA = "1001"
	validateExtB = "2002"
)

// The comparator's Station keys for the two seeded stations.
var (
	validateKeyA = parity.StationKey(seedSource, validateExtA)
	validateKeyB = parity.StationKey(seedSource, validateExtB)
)

const (
	// Divergent by design across the two sides.
	validateBaseA = int64(1)
	validateBaseB = int64(2)
	validateNewA  = int64(7)
	validateNewB  = int64(9)
)

// validateSeedRow is one seeded parsing_warnings row: the station's
// external_id ("" for a NULL station_id), the rule, severity, issue.
type validateSeedRow struct {
	station  string
	rule     string
	severity string
	issue    string
}

// validateSeedRows is the baseline validate row set, identical on both
// sides. The diurnal daily-sanity tuple is seeded twice on purpose.
var validateSeedRows = []validateSeedRow{
	{"", "orphan-runs", "warn", "power_runs.id=1 started 2026-06-09T00:00:00Z, status='running' beyond 24h0m0s — reconciled to 'aborted'"},
	{validateExtA, "bbox", "warn", `station conagua_conventional/1001 ("AGUASCALIENTES (OBS)") has NULL lat`},
	{validateExtB, "bbox", "error", `station conagua_conventional/2002 ("BROKEN") at impossible lat=0.0000 lon=0.0000 — error blocks publish`},
	{validateExtA, "wmo-month-completeness", "warn", "WMO §4.4.1 fail in period 1991-2020, calendar month=01: 1 year(s) violate (≥11 missing or ≥5 consecutive); top: 1992 (11/31 missing, gap=11)"},
	{validateExtB, "wmo-month-completeness", "warn", "WMO §4.4.1 fail in period 1991-2020, calendar month=02: 2 year(s) violate (≥11 missing or ≥5 consecutive); top: 1993 (5/28 missing, gap=5); 1994 (28/28 missing, gap=28)"},
	{validateExtB, "daily-sanity", "warn", "diurnal range > 25°C on 3 day(s); top: 1995-01-31 (35.0°C); 1995-02-01 (27.5°C); 1995-02-02 (26.0°C)"},
	{validateExtB, "daily-sanity", "warn", "diurnal range > 25°C on 3 day(s); top: 1995-01-31 (35.0°C); 1995-02-01 (27.5°C); 1995-02-02 (26.0°C)"},
	{validateExtB, "daily-sanity", "warn", "tmax outliers > 3.0σ in calendar month=05: 1 day(s) (mean=22.4 σ=2.3); top: 1995-05-31 tmax=35.0 (+5.5σ)"},
	{validateExtA, "cross-period", "warn", "tmax cross-period delta exceeds ±3.0°C in 1 (month, period-pair)(s); top: m=01 1981-2010 vs 1991-2020: 25.0 vs 30.0 (Δ-5.0°C)"},
}

// validateWarningFor builds the comparator's key for a seeded row: the
// station's natural key under the seed source, or empty for NULL.
func validateWarningFor(r validateSeedRow) parity.ValidateWarning {
	station := ""
	if r.station != "" {
		station = parity.StationKey(seedSource, r.station)
	}
	return parity.ValidateWarning{
		Station:    station,
		SourceFile: "validate:" + r.rule,
		Severity:   r.severity,
		Issue:      r.issue,
	}
}

// seedValidateDB seeds one side: the two stations under the given
// surrogate ids, every baseline validate row, and one ingest-written
// warning (source_file 'daily/1001.txt', line 57) the comparator must
// leave out of every tally.
func seedValidateDB(db *sql.DB, idA, idB int64) {
	GinkgoHelper()
	execSQL(db, `INSERT INTO stations (id, source, external_id, name, lat, lon)
		VALUES (?, ?, ?, 'AGUASCALIENTES (OBS)', NULL, -102.2908333)`, idA, seedSource, validateExtA)
	execSQL(db, `INSERT INTO stations (id, source, external_id, name, lat, lon)
		VALUES (?, ?, ?, 'BROKEN', 0, 0)`, idB, seedSource, validateExtB)
	for _, r := range validateSeedRows {
		insertValidateRow(db, r)
	}
	execSQL(db, `INSERT INTO parsing_warnings (station_id, source_file, line, severity, issue)
		VALUES (?, 'daily/1001.txt', 57, 'warn', 'unparseable TMAX')`, idA)
}

// insertValidateRow inserts one validate row, resolving the station's
// surrogate through the stations table so the same seed row lands under
// each side's own id.
func insertValidateRow(db *sql.DB, r validateSeedRow) {
	GinkgoHelper()
	var stationID any
	if r.station != "" {
		var id int64
		Expect(db.QueryRow(`SELECT id FROM stations WHERE source = ? AND external_id = ?`,
			seedSource, r.station).Scan(&id)).To(Succeed(), r.station)
		stationID = id
	}
	execSQL(db, `INSERT INTO parsing_warnings (station_id, source_file, line, severity, issue)
		VALUES (?, ?, NULL, ?, ?)`, stationID, "validate:"+r.rule, r.severity, r.issue)
}

// seedValidatePair creates both sides with the same rows under
// divergent surrogate ids.
func seedValidatePair() (baseDB, newDB *sql.DB) {
	GinkgoHelper()
	baseDB = openIngestDB()
	newDB = openIngestDB()
	seedValidateDB(baseDB, validateBaseA, validateBaseB)
	seedValidateDB(newDB, validateNewA, validateNewB)
	return baseDB, newDB
}

func mustCompareValidate(ctx context.Context, baseDB, newDB *sql.DB) *parity.ValidateComparison {
	GinkgoHelper()
	cmp, err := parity.CompareValidate(ctx, baseDB, newDB)
	Expect(err).NotTo(HaveOccurred())
	return cmp
}

// validateRule fetches one reference rule's tally, failing the spec
// when the id is not a reference rule.
func validateRule(cmp *parity.ValidateComparison, id string) parity.ValidateRule {
	GinkgoHelper()
	r, ok := cmp.Rule(id)
	Expect(ok).To(BeTrue(), id)
	return r
}

// expectValidateRuleClean asserts one rule compared identical with the
// given row count on both sides.
func expectValidateRuleClean(r parity.ValidateRule, rows int) {
	GinkgoHelper()
	Expect(r.BaseRows).To(Equal(rows), "%s base rows", r.ID)
	Expect(r.NewRows).To(Equal(rows), "%s new rows", r.ID)
	Expect(r.Identical).To(Equal(rows), "%s identical", r.ID)
	Expect(r.BaseOnly.Total).To(BeZero(), "%s base-only", r.ID)
	Expect(r.NewOnly.Total).To(BeZero(), "%s new-only", r.ID)
	Expect(r.BaseOnlyRows).To(BeZero(), "%s base-only rows", r.ID)
	Expect(r.NewOnlyRows).To(BeZero(), "%s new-only rows", r.ID)
	Expect(r.Clean()).To(BeTrue(), r.ID)
}

// validateSeedRowsFor returns the baseline rows of one rule.
func validateSeedRowsFor(rule string) []validateSeedRow {
	var out []validateSeedRow
	for _, r := range validateSeedRows {
		if r.rule == rule {
			out = append(out, r)
		}
	}
	return out
}
