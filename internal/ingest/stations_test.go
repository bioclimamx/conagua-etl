package ingest_test

import (
	"context"
	"database/sql"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/ingest"
	"github.com/bioclimamx/conagua-etl/internal/snapshot"
)

var _ = Describe("UpsertStation", func() {
	ctx := context.Background()

	It("inserts then enriches with COALESCE/CASE merge semantics", func() {
		db := openDB(filepath.Join(GinkgoT().TempDir(), "bioclima.db"))
		tx := beginTx(db)

		// Seed: catalog-style row, no coordinates.
		id1, err := ingest.UpsertStation(ctx, tx, ingest.StationUpsert{
			Source:       ingest.SourceConaguaConventional,
			ExternalID:   "1001",
			Name:         "AGUASCALIENTES (OBS)",
			State:        "AGS",
			Municipality: "AGUASCALIENTES",
			Status:       "operating",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(id1).NotTo(BeZero())

		// Enrich: header-style row — coordinates only, the rest blank.
		// Must return the same surrogate id and must not zero the seed
		// fields.
		id2, err := ingest.UpsertStation(ctx, tx, ingest.StationUpsert{
			Source:     ingest.SourceConaguaConventional,
			ExternalID: "1001",
			Name:       "AGUASCALIENTES (OBS)",
			Lat:        f64(21.85027778),
			Lon:        f64(-102.2908333),
			AltitudeM:  f64(1890.8),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(id2).To(Equal(id1), "same natural key must return the same surrogate id")

		var name string
		var state, muni, status sql.NullString
		var lat, lon, alt sql.NullFloat64
		Expect(tx.QueryRowContext(ctx, `SELECT name, state, municipality, status, lat, lon, altitude_m
			FROM stations WHERE id = ?`, id1).
			Scan(&name, &state, &muni, &status, &lat, &lon, &alt)).To(Succeed())
		Expect(name).To(Equal("AGUASCALIENTES (OBS)"))
		Expect(state).To(Equal(validStr("AGS")))
		Expect(muni).To(Equal(validStr("AGUASCALIENTES")))
		Expect(status).To(Equal(validStr("operating")))
		Expect(lat).To(Equal(validFloat(21.85027778)))
		Expect(lon).To(Equal(validFloat(-102.2908333)))
		Expect(alt).To(Equal(validFloat(1890.8)))
	})

	It("lets a non-empty incoming name win over the stored one", func() {
		db := openDB(filepath.Join(GinkgoT().TempDir(), "bioclima.db"))
		tx := beginTx(db)

		id, err := ingest.UpsertStation(ctx, tx, ingest.StationUpsert{
			Source: ingest.SourceConaguaConventional, ExternalID: "1001",
			Name: "Catalog Name", State: "AGS",
		})
		Expect(err).NotTo(HaveOccurred())

		_, err = ingest.UpsertStation(ctx, tx, ingest.StationUpsert{
			Source: ingest.SourceConaguaConventional, ExternalID: "1001",
			Name: "HEADER NAME",
		})
		Expect(err).NotTo(HaveOccurred())

		var name string
		var state sql.NullString
		Expect(tx.QueryRowContext(ctx, `SELECT name, state FROM stations WHERE id = ?`, id).
			Scan(&name, &state)).To(Succeed())
		Expect(name).To(Equal("HEADER NAME"), "non-empty incoming values always win")
		Expect(state).To(Equal(validStr("AGS")), "fields absent from the update stay put")
	})

	It("keeps rows under different sources distinct for the same external_id", func() {
		db := openDB(filepath.Join(GinkgoT().TempDir(), "bioclima.db"))
		tx := beginTx(db)

		id1, err := ingest.UpsertStation(ctx, tx, ingest.StationUpsert{
			Source: ingest.SourceConaguaConventional, ExternalID: "1001", Name: "A",
		})
		Expect(err).NotTo(HaveOccurred())
		id2, err := ingest.UpsertStation(ctx, tx, ingest.StationUpsert{
			Source: ingest.SourceNASAPower, ExternalID: "1001", Name: "B",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(id2).NotTo(Equal(id1))

		var nameA, nameB string
		Expect(tx.QueryRowContext(ctx, `SELECT name FROM stations WHERE id = ?`, id1).Scan(&nameA)).To(Succeed())
		Expect(tx.QueryRowContext(ctx, `SELECT name FROM stations WHERE id = ?`, id2).Scan(&nameB)).To(Succeed())
		Expect(nameA).To(Equal("A"))
		Expect(nameB).To(Equal("B"))
	})

	It("rejects empty required fields before touching the DB", func() {
		db := openDB(filepath.Join(GinkgoT().TempDir(), "bioclima.db"))
		tx := beginTx(db)

		for _, s := range []ingest.StationUpsert{
			{Source: "", ExternalID: "1", Name: "x"},
			{Source: ingest.SourceConaguaConventional, ExternalID: "", Name: "x"},
			{Source: ingest.SourceConaguaConventional, ExternalID: "1", Name: ""},
		} {
			_, err := ingest.UpsertStation(ctx, tx, s)
			Expect(err).To(HaveOccurred(), "upsert %+v must be rejected", s)
		}
	})
})

var _ = Describe("Run with StationsOnly (the station seed)", func() {
	ctx := context.Background()
	const date = "2026-04-21"

	seedIndex := func(metaRoot string) {
		writeIndexFile(metaRoot, date, []snapshot.StationProgress{
			{State: "ags", ID: "01001", Name: "AGUASCALIENTES (OBS)",
				Municipality: "AGUASCALIENTES", Status: "operating"},
			{State: "yuc", ID: "76225", Name: "MÉRIDA",
				Municipality: "MÉRIDA", Status: "operating"},
		})
	}

	It("seeds short-form ids with uppercased state and NULL coordinates, and writes no run row", func() {
		dir := GinkgoT().TempDir()
		metaRoot := filepath.Join(dir, "meta")
		dbPath := filepath.Join(dir, "bioclima.db")
		seedIndex(metaRoot)

		opts := runOpts(snapshot.NewLocalFS(filepath.Join(dir, "data")), metaRoot, date, dbPath)
		opts.StationsOnly = true
		report, err := ingest.Run(ctx, opts)
		Expect(err).NotTo(HaveOccurred())
		Expect(report.StationsSeeded).To(Equal(2))
		Expect(report.IngestRunID).To(BeZero(), "--stations-only must not open an ingest_runs row")

		db := openDB(dbPath)
		Expect(countRows(db, `SELECT COUNT(*) FROM ingest_runs`)).To(BeZero())
		Expect(countRows(db, `SELECT COUNT(*) FROM stations`)).To(Equal(2))

		// Catalog "01001" seeds under the short form "1001".
		ags := selectStation(db, "conagua_conventional", "1001")
		Expect(ags.Source).To(Equal("conagua_conventional"))
		Expect(ags.ExternalID).To(Equal("1001"))
		Expect(ags.Name).To(Equal("AGUASCALIENTES (OBS)"))
		Expect(ags.State).To(Equal(validStr("AGS")), "catalog slug uppercased")
		Expect(ags.Municipality).To(Equal(validStr("AGUASCALIENTES")))
		Expect(ags.Status).To(Equal(validStr("operating")))
		Expect(ags.Lat).To(Equal(sql.NullFloat64{}), "coordinates stay NULL at seed time")
		Expect(ags.Lon).To(Equal(sql.NullFloat64{}))
		Expect(ags.AltitudeM).To(Equal(sql.NullFloat64{}))

		// Already-short "76225" passes through unchanged.
		yuc := selectStation(db, "conagua_conventional", "76225")
		Expect(yuc.ExternalID).To(Equal("76225"))
		Expect(yuc.Name).To(Equal("MÉRIDA"))
		Expect(yuc.State).To(Equal(validStr("YUC")))
	})

	It("reseeds idempotently: same surrogate ids, same row count", func() {
		dir := GinkgoT().TempDir()
		metaRoot := filepath.Join(dir, "meta")
		dbPath := filepath.Join(dir, "bioclima.db")
		seedIndex(metaRoot)

		opts := runOpts(snapshot.NewLocalFS(filepath.Join(dir, "data")), metaRoot, date, dbPath)
		opts.StationsOnly = true
		_, err := ingest.Run(ctx, opts)
		Expect(err).NotTo(HaveOccurred())

		db := openDB(dbPath)
		id1001 := stationID(db, "conagua_conventional", "1001")
		id76225 := stationID(db, "conagua_conventional", "76225")

		_, err = ingest.Run(ctx, opts)
		Expect(err).NotTo(HaveOccurred())

		Expect(stationID(db, "conagua_conventional", "1001")).To(Equal(id1001))
		Expect(stationID(db, "conagua_conventional", "76225")).To(Equal(id76225))
		Expect(countRows(db, `SELECT COUNT(*) FROM stations`)).To(Equal(2))
	})
})
