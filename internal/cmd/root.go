// Package cmd defines the conagua-etl cobra command tree — the single
// source of truth for the CLI surface. Each
// subcommand lives in its own file; this file holds the root command, the
// persistent --root flag every subcommand shares, and the version surface.
package cmd

import (
	"errors"
	"fmt"
	"runtime/debug"

	"github.com/spf13/cobra"
)

// snapshotRootFlag points at the directory that holds the local snapshot
// metadata (_progress.json, _index.json) and, with --sink=local, the raw
// station files too.
var snapshotRootFlag string

var rootCmd = &cobra.Command{
	Use:   "conagua-etl",
	Short: "Offline ETL for CONAGUA climatological data",
	Long: "conagua-etl is the offline pipeline that snapshots CONAGUA's weather\n" +
		"station archive, ingests it into SQLite, augments it with NASA POWER\n" +
		"reanalysis, and publishes citable release artifacts.",
	Version: versionString(),
	// Errors surface exactly once, through main's single "error:" line;
	// cobra's own error print and the usage dump would triple the noise.
	SilenceUsage:  true,
	SilenceErrors: true,
}

// Execute runs the root command and returns its error (nil on success).
// main prints the error once and maps it to a process exit code via
// ExitCode — library code never calls os.Exit.
func Execute() error {
	return rootCmd.Execute()
}

// DegradedError marks a run that completed but left some units failed —
// the fail-soft casualties already surfaced in the run report. It is a
// distinct exit-code signal (2) so unattended cron/CI can tell "finished
// with losses" apart from both a clean run and a run-level failure.
type DegradedError struct {
	// Summary is the one-line description main prints as the error line.
	Summary string
}

func (e DegradedError) Error() string { return e.Summary }

// ExitCode maps the root command's result to the process exit code:
// 0 success, 2 degraded-but-completed (DegradedError anywhere in the
// chain), 1 any other failure.
func ExitCode(err error) int {
	if err == nil {
		return 0
	}
	var degraded DegradedError
	if errors.As(err, &degraded) {
		return 2
	}
	return 1
}

func init() {
	rootCmd.PersistentFlags().StringVar(&snapshotRootFlag, "root", "./snapshots",
		"Local filesystem root for snapshot metadata (and, with --sink=local, data).")
	rootCmd.CompletionOptions.HiddenDefaultCmd = true

	rootCmd.AddCommand(pullCmd)
	rootCmd.AddCommand(mirrorCmd)
	rootCmd.AddCommand(ingestCmd)
	rootCmd.AddCommand(powerCmd)
	rootCmd.AddCommand(validateCmd)
	rootCmd.AddCommand(publishCmd)
}

// versionString assembles the --version output from build metadata. Both
// parts are best-effort: a build outside a git checkout (or with
// -buildvcs=off) simply omits the SHA, and a non-module build falls back
// to "dev".
func versionString() string {
	version := "dev"
	if bi, ok := debug.ReadBuildInfo(); ok {
		if v := bi.Main.Version; v != "" && v != "(devel)" {
			version = v
		}
	}
	sha := buildGitSHA()
	if sha == "" {
		return version
	}
	return fmt.Sprintf("%s (git %s)", version, sha)
}

// buildGitSHA returns the vcs.revision stamped into the binary, or "" when
// the build carried no VCS metadata. pull persists it into the snapshot
// ledger (ETLGitSHA), ingest into ingest_runs and publish into
// manifest.json (etl_git_sha), so every durable artifact records the
// code that produced it. The SHA stays the bare revision — the ledgers
// record exactly the commit, never a decorated form — so a consumer can
// paste it into `git show`; whether the tree was modified travels
// separately, through buildVCS.
func buildGitSHA() string {
	revision, _ := buildVCS()
	return revision
}

// buildVCS reports the VCS provenance the toolchain stamped into the
// binary: the commit it was built from and whether that working tree
// carried uncommitted changes at build time. Both are zero when the
// build carried no VCS metadata at all (-buildvcs=off, `go run` or a
// build outside a checkout) — an absent revision is an honest gap the
// artifacts record as empty, which is a different thing from a revision
// that names code other than the code that ran. publish refuses the
// latter (checkBuildProvenance); every verb records the former.
func buildVCS() (revision string, modified bool) {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return "", false
	}
	for _, s := range bi.Settings {
		switch s.Key {
		case "vcs.revision":
			revision = s.Value
		case "vcs.modified":
			modified = s.Value == "true"
		}
	}
	return revision, modified
}
