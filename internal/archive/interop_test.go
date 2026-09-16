package archive_test

// The archive → extractor seam. archive/zip reading back what archive/zip
// wrote proves consistency, not interoperability; a Zenodo user opens
// the deposit with whatever extractor they have. Info-ZIP unzip is the
// reference implementation on every Unix — the counterpart of the
// `sha256sum -c` spec for CHECKSUMS.

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/archive"
)

var _ = Describe("WriteZip interoperability", func() {
	It("is accepted whole by Info-ZIP unzip, listing and extracting every entry as written", func() {
		if _, err := exec.LookPath("unzip"); err != nil {
			Skip("unzip not on PATH")
		}
		dir := GinkgoT().TempDir()
		path := filepath.Join(dir, "yuc-tabular.zip")
		contents := map[string]string{
			"conagua/daily_observations/yuc/daily-31001.csv": "station_id,date,tmax_c,tmin_c,precip_mm,evap_mm\n" +
				strings.Repeat("31001,1981-01-01,31.0,14.5,0.0,\n", 50),
			"conagua/monthly_normals.csv": "station_id,period,month,tmax_c\n31001,1981-2010,1,27.4\n",
			"conagua/stations.csv":        "station_id,name,state\n31001,\"MÉRIDA, OBS\",YUC\n",
		}
		paths := []string{
			"conagua/daily_observations/yuc/daily-31001.csv",
			"conagua/monthly_normals.csv",
			"conagua/stations.csv",
		}
		var entries []archive.Entry
		for _, p := range paths {
			entries = append(entries, stringEntry(p, contents[p]))
		}
		_, err := archive.WriteZip(context.Background(), path, entries,
			archive.ZipOptions{Modified: time.Date(2026, 6, 8, 0, 0, 0, 0, time.UTC)})
		Expect(err).NotTo(HaveOccurred())

		out, err := exec.Command("unzip", "-t", path).CombinedOutput()
		Expect(err).NotTo(HaveOccurred(), string(out))
		Expect(string(out)).To(ContainSubstring("No errors detected"))

		list, err := exec.Command("unzip", "-Z1", path).CombinedOutput()
		Expect(err).NotTo(HaveOccurred(), string(list))
		Expect(strings.Split(strings.TrimSpace(string(list)), "\n")).To(Equal(paths))

		extract := filepath.Join(dir, "x")
		out, err = exec.Command("unzip", "-q", path, "-d", extract).CombinedOutput()
		Expect(err).NotTo(HaveOccurred(), string(out))
		for _, p := range paths {
			got, err := os.ReadFile(filepath.Join(extract, filepath.FromSlash(p)))
			Expect(err).NotTo(HaveOccurred(), p)
			Expect(string(got)).To(Equal(contents[p]), p)
		}
	})
})
