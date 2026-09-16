package publish_test

import (
	"archive/zip"
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/archive"
	"github.com/bioclimamx/conagua-etl/internal/publish"
)

// render writes one entry into memory and returns its bytes.
func render(e archive.Entry) []byte {
	GinkgoHelper()
	var buf bytes.Buffer
	Expect(e.Write(&buf)).To(Succeed(), e.Path)
	return buf.Bytes()
}

// records parses CSV bytes back into rows (header first) with the
// standard reader, so what a consumer sees is what is asserted.
func records(data []byte) [][]string {
	GinkgoHelper()
	rows, err := csv.NewReader(bytes.NewReader(data)).ReadAll()
	Expect(err).NotTo(HaveOccurred())
	return rows
}

func entryByPath(entries []archive.Entry, path string) archive.Entry {
	GinkgoHelper()
	for _, e := range entries {
		if e.Path == path {
			return e
		}
	}
	Fail("no entry at " + path)
	return archive.Entry{}
}

var yucatan = publish.State{Code: "YUC", Slug: "yuc", Name: "Yucatán"}

var _ = Describe("ConaguaEntries", func() {
	var (
		db      *sql.DB
		ids     map[string]int64
		entries []archive.Entry
		units   []string
	)

	BeforeEach(func() {
		db = openTempDB()
		ids = seedTwoStates(db)
		units = nil
		var err error
		entries, err = publish.ConaguaEntries(context.Background(), db, yucatan,
			func(path string) { units = append(units, path) })
		Expect(err).NotTo(HaveOccurred())
	})

	It("lists the four conagua/ files Path-sorted, one daily file per station under the state slug", func() {
		var paths []string
		for _, e := range entries {
			paths = append(paths, e.Path)
		}
		Expect(paths).To(Equal([]string{
			"conagua/daily_observations/yuc/daily-31001.csv",
			"conagua/daily_observations/yuc/daily-31002.csv",
			"conagua/daily_observations/yuc/daily-3101.csv",
			"conagua/monthly_normals.csv",
			"conagua/monthly_normals_extras.csv",
			"conagua/stations.csv",
		}))
	})

	It("round-trips every stations column, bytewise-sorted by station_id, this state and source only", func() {
		got := records(render(entryByPath(entries, "conagua/stations.csv")))
		Expect(got).To(Equal([][]string{
			publish.Stations.Header(),
			{"31001", `Mérida, "La Plancha"`, "YUC", "Mérida", "21.850278", "-89.375000", "9.0", "operating",
				"1951", "2026", "0.9722", "0.5000", "", "1.0000", "0.9324", "0.0000", "", "0.0009"},
			{"31002", "Tizimín", "YUC", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
			{"3101", "Valladolid", "YUC", "Valladolid", "20.689100", "-88.201100", "25.5", "suspended",
				"1961", "1995", "0.2500", "", "", "", "", "", "", ""},
		}))
	})

	It("round-trips every monthly_normals column in (station_id, period, month) order", func() {
		got := records(render(entryByPath(entries, "conagua/monthly_normals.csv")))
		Expect(got).To(Equal([][]string{
			publish.MonthlyNormals.Header(),
			{"31001", "1981-2010", "1", "33.4", "17.9", "25.7", "28.3", "141.6"},
			{"31001", "1981-2010", "12", "30.1", "16.2", "", "24.5", "110.3"},
			{"31001", "1991-2020", "1", "33.0", "18.3", "25.8", "0.0", "150.2"},
			{"31001", "1991-2020", "12", "", "", "", "", ""},
			{"3101", "1961-1990", "6", "35.9", "22.1", "29.0", "152.7", "190.4"},
		}))
	})

	It("round-trips every monthly_normals_extras column in (station_id, period, month) order", func() {
		got := records(render(entryByPath(entries, "conagua/monthly_normals_extras.csv")))
		Expect(got).To(Equal([][]string{
			publish.MonthlyNormalsExtras.Header(),
			{"31001", "1981-2010", "1",
				"38.5", "1998", "42.00", "1998-05-14",
				"8.2", "1985", "3.50", "1985-01-20",
				"210.6", "2002", "98.70", "2002-09-23",
				"28", "27", "27", "30", "25", "4.6", "29"},
			{"31001", "1981-2010", "12",
				"", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
			{"31001", "1991-2020", "1",
				"39.0", "", "", "",
				"", "", "", "",
				"190.2", "2012", "", "",
				"30", "", "", "30", "", "", ""},
			{"3101", "1961-1990", "6",
				"40.2", "1975", "", "",
				"", "", "", "",
				"", "", "", "",
				"", "", "", "", "", "12.2", "30"},
		}))
	})

	It("round-trips each station's daily rows by date, header-only for a station without observations", func() {
		Expect(string(render(entryByPath(entries, "conagua/daily_observations/yuc/daily-31001.csv")))).To(Equal(
			"station_id,date,tmax_c,tmin_c,precip_mm,evap_mm\n" +
				"31001,1999-12-31,,-0.04,,\n" +
				"31001,2020-01-01,30.50,,12.40,\n" +
				"31001,2020-01-02,31.00,19.50,0.00,4.20\n"))
		Expect(string(render(entryByPath(entries, "conagua/daily_observations/yuc/daily-31002.csv")))).To(Equal(
			"station_id,date,tmax_c,tmin_c,precip_mm,evap_mm\n"))
		Expect(records(render(entryByPath(entries, "conagua/daily_observations/yuc/daily-3101.csv")))).To(Equal([][]string{
			publish.DailyObservations.Header(),
			{"3101", "1975-06-15", "36.20", "22.00", "45.10", "7.30"},
		}))
	})

	It("carries a trace rain day to the artifact as 0.01, never collapsed to a dry 0.0", func() {
		// CONAGUA publishes "inappreciable" rain as 0.01 mm. Rounding the
		// observed columns to one decimal printed those days as 0.0 — a
		// dry day — while the shipped SQLite held the trace; carrying the
		// second decimal is what the daily columns' precision is for.
		insertDaily(db, ids["conv/31001"], "2020-01-03", 31.0, 19.0, 0.01, 0.0)
		data := string(render(entryByPath(entries, "conagua/daily_observations/yuc/daily-31001.csv")))
		Expect(data).To(Equal(
			"station_id,date,tmax_c,tmin_c,precip_mm,evap_mm\n" +
				"31001,1999-12-31,,-0.04,,\n" +
				"31001,2020-01-01,30.50,,12.40,\n" +
				"31001,2020-01-02,31.00,19.50,0.00,4.20\n" +
				"31001,2020-01-03,31.00,19.00,0.01,0.00\n"))
		// The trace day and the true-zero day stay distinguishable.
		Expect(data).NotTo(ContainSubstring("2020-01-03,31.00,19.00,0.0,"))
	})

	It("fires onUnit with the entry path once per station, in write order, as each daily entry is written", func() {
		for _, e := range entries {
			render(e)
		}
		Expect(units).To(Equal([]string{
			"conagua/daily_observations/yuc/daily-31001.csv",
			"conagua/daily_observations/yuc/daily-31002.csv",
			"conagua/daily_observations/yuc/daily-3101.csv",
		}))
	})

	It("writes UTF-8 without BOM, LF line endings, a header row, and RFC-4180 quoting", func() {
		data := render(entryByPath(entries, "conagua/stations.csv"))
		Expect(data).NotTo(HavePrefix("\xef\xbb\xbf"))
		Expect(bytes.Contains(data, []byte("\r"))).To(BeFalse())
		Expect(data).To(HaveSuffix("\n"))
		lines := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
		Expect(lines[0]).To(Equal(strings.Join(publish.Stations.Header(), ",")))
		Expect(lines[1]).To(HavePrefix(`31001,"Mérida, ""La Plancha""",YUC,Mérida,21.850278,`))
		// NULL is the empty field; a text NULL and a numeric NULL look
		// the same, and an all-NULL tail is a run of bare commas.
		Expect(lines[2]).To(Equal("31002,Tizimín,YUC,,,,,,,,,,,,,,,"))
	})

	It("scopes another state to its own stations", func() {
		ags := publish.State{Code: "AGS", Slug: "ags", Name: "Aguascalientes"}
		agsEntries, err := publish.ConaguaEntries(context.Background(), db, ags, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(agsEntries).To(HaveLen(4))
		Expect(agsEntries[0].Path).To(Equal("conagua/daily_observations/ags/daily-1001.csv"))
		Expect(records(render(entryByPath(agsEntries, "conagua/stations.csv")))).To(Equal([][]string{
			publish.Stations.Header(),
			{"1001", "Aguascalientes (OBS)", "AGS", "", "21.880000", "-102.300000", "1878.0", "operating",
				"", "", "", "", "", "", "", "", "", ""},
		}))
		Expect(records(render(entryByPath(agsEntries, "conagua/daily_observations/ags/daily-1001.csv")))).To(Equal([][]string{
			publish.DailyObservations.Header(),
			{"1001", "2020-01-01", "20.00", "5.00", "0.00", "6.00"},
		}))
	})

	It("yields the three table files and no daily files for a state with no stations", func() {
		none := publish.State{Code: "ZAC", Slug: "zac", Name: "Zacatecas"}
		zacEntries, err := publish.ConaguaEntries(context.Background(), db, none, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(zacEntries).To(HaveLen(3))
		Expect(records(render(zacEntries[0]))).To(Equal([][]string{publish.MonthlyNormals.Header()}))
	})

	It("honours cancellation at write time", func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancelable, err := publish.ConaguaEntries(ctx, db, yucatan, nil)
		Expect(err).NotTo(HaveOccurred())
		cancel()
		err = entryByPath(cancelable, "conagua/stations.csv").Write(io.Discard)
		Expect(errors.Is(err, context.Canceled)).To(BeTrue(), err)
	})

	It("refuses to write a non-finite REAL rather than emit +Inf", func() {
		mustExec(db, `UPDATE stations SET lat = 9e999 WHERE external_id = '3101'`)
		err := entryByPath(entries, "conagua/stations.csv").Write(io.Discard)
		Expect(err).To(MatchError(ContainSubstring("conagua/stations: row 3: column lat: non-finite value +Inf")))
	})
})

var _ = Describe("ConaguaEntries through archive.WriteZip", func() {
	It("writes the entries into a zip whose bytes are identical on a second build (byte-reproducible)", func() {
		db := openTempDB()
		seedTwoStates(db)
		entries, err := publish.ConaguaEntries(context.Background(), db, yucatan, nil)
		Expect(err).NotTo(HaveOccurred())

		dir := GinkgoT().TempDir()
		opts := archive.ZipOptions{Modified: time.Date(2026, 8, 29, 0, 0, 0, 0, time.UTC)}
		first, err := archive.WriteZip(context.Background(), filepath.Join(dir, "a.zip"), entries, opts)
		Expect(err).NotTo(HaveOccurred())
		second, err := archive.WriteZip(context.Background(), filepath.Join(dir, "b.zip"), entries, opts)
		Expect(err).NotTo(HaveOccurred())
		Expect(second).To(Equal(first))
		Expect(first.Entries).To(Equal(len(entries)))

		zr, err := zip.OpenReader(filepath.Join(dir, "a.zip"))
		Expect(err).NotTo(HaveOccurred())
		defer func() { _ = zr.Close() }()
		Expect(zr.File).To(HaveLen(len(entries)))
		for i, f := range zr.File {
			Expect(f.Name).To(Equal(entries[i].Path))
			rc, err := f.Open()
			Expect(err).NotTo(HaveOccurred())
			got, err := io.ReadAll(rc)
			Expect(err).NotTo(HaveOccurred())
			Expect(rc.Close()).To(Succeed())
			Expect(got).To(Equal(render(entries[i])), f.Name)
		}
	})
})
