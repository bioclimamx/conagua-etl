package ingest_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/conagua"
	"github.com/bioclimamx/conagua-etl/internal/ingest"
	"github.com/bioclimamx/conagua-etl/internal/snapshot"
)

var _ = Describe("Run", func() {
	ctx := context.Background()
	const date = "2026-04-21"

	Describe("options validation", func() {
		valid := func() ingest.Options {
			return ingest.Options{
				Sink:         snapshot.NewLocalFS(GinkgoT().TempDir()),
				MetadataRoot: "m", SnapshotDate: date, DBPath: "db",
			}
		}

		It("rejects each missing requirement and contradiction up front", func() {
			for name, mutate := range map[string]func(*ingest.Options){
				"nil Sink":              func(o *ingest.Options) { o.Sink = nil },
				"empty MetadataRoot":    func(o *ingest.Options) { o.MetadataRoot = "" },
				"empty SnapshotDate":    func(o *ingest.Options) { o.SnapshotDate = "" },
				"empty DBPath":          func(o *ingest.Options) { o.DBPath = "" },
				"StationsOnly + Kind":   func(o *ingest.Options) { o.StationsOnly = true; o.Kind = "daily" },
				"unknown Kind monthly":  func(o *ingest.Options) { o.Kind = "monthly" },
				"unknown Kind gibberis": func(o *ingest.Options) { o.Kind = "gibberish" },
			} {
				o := valid()
				mutate(&o)
				_, err := ingest.Run(ctx, o)
				Expect(err).To(HaveOccurred(), "case %s must be rejected", name)
			}
		})
	})

	Describe("a pull-errored file", func() {
		// One station whose only catalog entry errored during pull.
		setup := func(dir string) (opts ingest.Options, sink *recordingSink) {
			metaRoot := filepath.Join(dir, "meta")
			writeIndexFile(metaRoot, date, []snapshot.StationProgress{{
				State: "bcs", ID: "03074", Name: "La Paz (Dge)",
				Municipality: "La Paz", Status: "operating",
				Files: map[conagua.Kind]snapshot.FileState{
					conagua.KindNormals1991_2020: {
						URL:       "https://smn.conagua.gob.mx/.../nor9120_03074.txt",
						Outcome:   snapshot.OutcomeError,
						HTTPCode:  500,
						Attempts:  3,
						LastError: "unexpected status 500",
					},
				},
			}})
			sink = &recordingSink{inner: snapshot.NewLocalFS(filepath.Join(dir, "data"))}
			opts = runOpts(sink, metaRoot, date, filepath.Join(dir, "bioclima.db"))
			opts.ExternalID = "3074"
			opts.Kind = "normals"
			return opts, sink
		}

		It("lands as one warn row, never touches the sink, and counts as pull-errored", func() {
			dir := GinkgoT().TempDir()
			opts, sink := setup(dir)

			report, err := ingest.Run(ctx, opts)
			Expect(err).NotTo(HaveOccurred())
			Expect(report.FilesPullErrored).To(Equal(1))
			Expect(report.FilesOpened).To(BeZero(), "no file may be read for a pull-errored kind")
			Expect(sink.getCalls).To(BeZero(), "sink.Get must never be called")

			db := openDB(opts.DBPath)
			sid := stationID(db, "conagua_conventional", "3074")
			warns := selectWarnings(db)
			Expect(warns).To(Equal([]warningRow{{
				StationID:  validInt(sid),
				SourceFile: "conagua-raw/2026-04-21/normals_1991_2020/03074.txt",
				Line:       sql.NullInt64{}, // pull-error warnings carry no line anchor
				Severity:   "warn",
				Issue:      "pull errored after 3 attempt(s), http=500: unexpected status 500",
			}}))
		})

		It("does not duplicate the warn row on a re-run", func() {
			dir := GinkgoT().TempDir()
			opts, _ := setup(dir)

			_, err := ingest.Run(ctx, opts)
			Expect(err).NotTo(HaveOccurred())
			_, err = ingest.Run(ctx, opts)
			Expect(err).NotTo(HaveOccurred())

			db := openDB(opts.DBPath)
			sid := stationID(db, "conagua_conventional", "3074")
			warns := selectWarnings(db)
			Expect(warns).To(HaveLen(1), "re-ingest must clear and rewrite, not accumulate")
			Expect(warns[0].StationID).To(Equal(validInt(sid)))
			Expect(warns[0].Issue).To(Equal("pull errored after 3 attempt(s), http=500: unexpected status 500"))
			Expect(countRows(db, `SELECT COUNT(*) FROM ingest_runs`)).To(Equal(2))
		})
	})

	Describe("per-station transaction isolation", func() {
		It("commits station A's warning while rolling back all of failing station B", func() {
			dir := GinkgoT().TempDir()
			metaRoot := filepath.Join(dir, "meta")

			// A: one pull-errored normals kind → commits with a warn row.
			// B: same pull error PLUS a "fetched" daily whose bytes the
			// sink refuses → B's whole tx rolls back.
			writeIndexFile(metaRoot, date, []snapshot.StationProgress{
				{State: "bcs", ID: "03074", Name: "La Paz",
					Files: map[conagua.Kind]snapshot.FileState{
						conagua.KindNormals1991_2020: {
							Outcome: snapshot.OutcomeError, HTTPCode: 500, Attempts: 1,
							LastError: "upstream 500",
						},
					}},
				{State: "bcs", ID: "03075", Name: "San José",
					Files: map[conagua.Kind]snapshot.FileState{
						conagua.KindNormals1991_2020: {
							Outcome: snapshot.OutcomeError, HTTPCode: 502, Attempts: 1,
							LastError: "upstream 502",
						},
						conagua.KindDaily: {
							Outcome: snapshot.OutcomeFetched, HTTPCode: 200, Attempts: 1,
						},
					}},
			})

			sink := &erroringSink{
				inner:         snapshot.NewLocalFS(filepath.Join(dir, "data")),
				errForStation: "03075",
				errForKind:    conagua.KindDaily,
				errToReturn:   errors.New("simulated sink failure"),
			}
			opts := runOpts(sink, metaRoot, date, filepath.Join(dir, "bioclima.db"))

			report, err := ingest.Run(ctx, opts)
			Expect(err).NotTo(HaveOccurred(), "per-station failures must not bubble")
			Expect(sink.callsIntercept).NotTo(BeZero(), "fault injector never hit — scaffolding broken")

			Expect(report.StationsAttempted).To(Equal(2))
			Expect(report.StationsSucceeded).To(Equal(1))
			Expect(report.StationsFailed).To(Equal(1))
			Expect(report.PerStationFailures).To(HaveLen(1))
			Expect(report.PerStationFailures[0].ExternalID).To(Equal("3075"), "failure carries the SHORT-form id")
			Expect(report.PerStationFailures[0].Name).To(Equal("San José"))
			Expect(report.PerStationFailures[0].Err).To(MatchError(ContainSubstring("simulated sink failure")))

			db := openDB(opts.DBPath)

			// A committed: exactly its pull-error warning, values intact.
			idA := stationID(db, "conagua_conventional", "3074")
			warns := selectWarnings(db)
			Expect(warns).To(HaveLen(1))
			Expect(warns[0].StationID).To(Equal(validInt(idA)))
			Expect(warns[0].SourceFile).To(Equal("conagua-raw/2026-04-21/normals_1991_2020/03074.txt"))
			Expect(warns[0].Severity).To(Equal("warn"))
			Expect(warns[0].Issue).To(Equal("pull errored after 1 attempt(s), http=500: upstream 500"))

			// B was seeded (the seed tx is separate and commits first) but
			// none of its per-station work may exist in ANY observed table.
			idB := stationID(db, "conagua_conventional", "3075")
			for _, table := range []string{
				"parsing_warnings", "daily_observations", "monthly_normals", "monthly_normals_extras",
			} {
				Expect(countRows(db, `SELECT COUNT(*) FROM `+table+` WHERE station_id = ?`, idB)).
					To(BeZero(), "station B must have no rows in %s", table)
			}

			// The run row closed complete — per-station failure is
			// operational noise, not a run failure — with true counters.
			run := selectRun(db, report.IngestRunID)
			Expect(run.Status).To(Equal("complete"))
			Expect(run.SnapshotDate).To(Equal(date))
			Expect(run.SinkKind).To(Equal("local"))
			Expect(run.Attempted).To(Equal(validInt(2)))
			Expect(run.Succeeded).To(Equal(validInt(1)))
			Expect(run.Failed).To(Equal(validInt(1)))
			Expect(run.DailyRows).To(Equal(validInt(0)))
			Expect(run.NormalsRows).To(Equal(validInt(0)))
			Expect(run.ExtrasRows).To(Equal(validInt(0)))
			Expect(run.Warnings).To(Equal(validInt(1)), "only A's committed warning counts")
			Expect(run.FinishedAt.Valid).To(BeTrue())
		})
	})

	Describe("a ledger/sink divergence", func() {
		It("warns when the ledger says fetched but the sink has no object", func() {
			dir := GinkgoT().TempDir()
			metaRoot := filepath.Join(dir, "meta")
			writeIndexFile(metaRoot, date, []snapshot.StationProgress{{
				State: "bcs", ID: "03074", Name: "La Paz (Dge)", Status: "operating",
				Files: map[conagua.Kind]snapshot.FileState{
					conagua.KindDaily: {Outcome: snapshot.OutcomeFetched, HTTPCode: 200, Attempts: 1},
				},
			}})

			// The sink is empty: the object the ledger promised is gone.
			opts := runOpts(snapshot.NewLocalFS(filepath.Join(dir, "data")), metaRoot, date,
				filepath.Join(dir, "bioclima.db"))
			opts.ExternalID = "3074"
			opts.Kind = "daily"

			report, err := ingest.Run(ctx, opts)
			Expect(err).NotTo(HaveOccurred(), "a missing object is a data gap, not a run failure")
			Expect(report.FilesOpened).To(BeZero())
			Expect(report.StationsSucceeded).To(Equal(1), "the station still commits")

			db := openDB(opts.DBPath)
			sid := stationID(db, "conagua_conventional", "3074")
			Expect(selectWarnings(db)).To(Equal([]warningRow{{
				StationID:  validInt(sid),
				SourceFile: "conagua-raw/2026-04-21/daily/03074.txt",
				Line:       sql.NullInt64{},
				Severity:   "warn",
				Issue:      "pull ledger says fetched, but sink has no object at this key",
			}}))
		})
	})

	Describe("a kind absent from the catalog", func() {
		It("is silent: no warning, only the FilesNotInCatalog counter", func() {
			dir := GinkgoT().TempDir()
			metaRoot := filepath.Join(dir, "meta")
			writeIndexFile(metaRoot, date, []snapshot.StationProgress{{
				State: "bcs", ID: "03074", Name: "La Paz (Dge)", Status: "operating",
				Files: map[conagua.Kind]snapshot.FileState{},
			}})

			opts := runOpts(snapshot.NewLocalFS(filepath.Join(dir, "data")), metaRoot, date,
				filepath.Join(dir, "bioclima.db"))
			opts.ExternalID = "3074"

			report, err := ingest.Run(ctx, opts)
			Expect(err).NotTo(HaveOccurred())

			// Default kind set = daily + the four normals periods.
			Expect(report.FilesNotInCatalog).To(Equal(5))
			Expect(report.FilesPullErrored).To(BeZero())
			Expect(report.FilesOpened).To(BeZero())
			Expect(report.Warnings).To(BeEmpty())

			db := openDB(opts.DBPath)
			Expect(selectWarnings(db)).To(BeEmpty())
		})
	})

	Describe("the index bootstrap", func() {
		indexBody := []byte(`{"schema_version":1,"snapshot_date":"2026-04-21","stations":[]}`)

		It("copies the sink's index verbatim to the canonical path exactly once", func() {
			dir := GinkgoT().TempDir()
			metaRoot := filepath.Join(dir, "meta")
			indexPath := filepath.Join(metaRoot, "conagua-raw", date, "_index.json")

			sink := &indexFetcherSink{
				Sink:      snapshot.NewLocalFS(filepath.Join(dir, "data")),
				indexBody: indexBody,
			}
			opts := runOpts(sink, metaRoot, date, filepath.Join(dir, "bioclima.db"))
			opts.StationsOnly = true

			_, err := ingest.Run(ctx, opts)
			Expect(err).NotTo(HaveOccurred())
			Expect(sink.calls).To(Equal(1))
			got, err := os.ReadFile(indexPath)
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal(indexBody), "bootstrapped bytes must be verbatim")

			// Second run: the local copy exists → the sink is not asked again.
			_, err = ingest.Run(ctx, opts)
			Expect(err).NotTo(HaveOccurred())
			Expect(sink.calls).To(Equal(1), "bootstrap must be a no-op when the index exists")
		})

		It("stays quiet for a sink without FetchIndex and surfaces the missing index", func() {
			dir := GinkgoT().TempDir()
			metaRoot := filepath.Join(dir, "meta")
			indexPath := filepath.Join(metaRoot, "conagua-raw", date, "_index.json")

			opts := runOpts(snapshot.NewLocalFS(filepath.Join(dir, "data")), metaRoot, date,
				filepath.Join(dir, "bioclima.db"))
			opts.StationsOnly = true

			_, err := ingest.Run(ctx, opts)
			Expect(err).To(MatchError(ContainSubstring(indexPath)),
				"the error must point at the canonical index path")
			_, statErr := os.Stat(indexPath)
			Expect(statErr).To(MatchError(os.ErrNotExist),
				"the quiet no-op must not fabricate an index file")
		})
	})
})
