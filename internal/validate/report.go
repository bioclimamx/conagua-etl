package validate

import (
	"bytes"
	"fmt"
	"html/template"
	"io"
	"os"
	"time"

	"github.com/bioclimamx/conagua-etl/internal/archive"
)

// reportMode is the HTML report's file mode: world-readable. The
// atomic writer's temp file is created owner-only, which suits the
// deposit artifacts it was built for but not an operator-facing report
// meant to be opened and shared.
const reportMode = 0o644

// WriteHTMLReport renders r into a self-contained HTML file at path —
// no external assets, so it opens offline. The page is rendered in full
// before the file is touched and written atomically (temp sibling +
// rename), so neither a template failure nor a crash mid-write leaves a
// partial report at path.
func WriteHTMLReport(path string, r *Report) error {
	var buf bytes.Buffer
	if err := renderReport(&buf, r); err != nil {
		return fmt.Errorf("render report: %w", err)
	}
	if _, err := archive.WriteFileAtomic(path, func(w io.Writer) error {
		_, err := w.Write(buf.Bytes())
		return err
	}); err != nil {
		return fmt.Errorf("html report: %w", err)
	}
	if err := os.Chmod(path, reportMode); err != nil {
		return fmt.Errorf("html report: %w", err)
	}
	return nil
}

func renderReport(out io.Writer, r *Report) error {
	return reportTpl.Execute(out, r)
}

// reportTpl is the report page, parsed once. The asset is a literal, so
// a parse failure is an impossible state guarded at init.
var reportTpl = template.Must(template.New("report").Funcs(template.FuncMap{
	"fmtTime": func(t time.Time) string { return t.Format(time.RFC3339) },
	"elapsed": func(start, end time.Time) string {
		return end.Sub(start).Round(time.Millisecond).String()
	},
	"ms": func(d time.Duration) string { return d.Round(time.Millisecond).String() },
}).Parse(reportTemplate))

const reportTemplate = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Validate report — {{fmtTime .StartedAt}}</title>
<style>
  :root {
    --bg: #0e0f12; --panel: #16181d; --border: rgba(255,255,255,0.08);
    --ink: #ECEDEF; --ink2: #b0b2b7; --ink3: #74777E;
    --ok: #3aa55a; --warn: #c9b98a; --fail: #c0392b; --accent: #E8773C;
    --mono: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, monospace;
  }
  * { box-sizing: border-box; }
  html, body { margin: 0; padding: 0; background: var(--bg); color: var(--ink);
               font: 14px/1.55 system-ui, -apple-system, "Segoe UI", Roboto, sans-serif; }
  main { max-width: 1024px; margin: 0 auto; padding: 32px; }
  h1 { font-size: 28px; margin: 0 0 4px 0; font-weight: 600; }
  h2 { font-size: 18px; margin: 32px 0 12px 0; font-weight: 600; color: var(--ink); }
  h3 { font-size: 14px; margin: 20px 0 8px 0; font-weight: 600; color: var(--ink); }
  .sub { color: var(--ink2); margin: 0; }
  .panel { background: var(--panel); border: 1px solid var(--border);
           border-radius: 8px; padding: 16px 20px; margin: 16px 0; }
  .stats { display: grid; gap: 8px 32px; grid-template-columns: repeat(2, max-content 1fr); }
  .stats dt { color: var(--ink3); font-family: var(--mono); font-size: 12px;
              text-transform: uppercase; letter-spacing: 0.06em; }
  .stats dd { margin: 0; font-family: var(--mono); font-size: 16px; }
  .ok   { color: var(--ok); }
  .warn { color: var(--warn); }
  .fail { color: var(--fail); }
  table { width: 100%; border-collapse: collapse; font-family: var(--mono); font-size: 12px; }
  th, td { padding: 6px 12px; text-align: left; border-bottom: 1px solid var(--border);
           vertical-align: top; }
  th { color: var(--ink3); font-weight: 500; text-transform: uppercase; letter-spacing: 0.06em; }
  td.num { text-align: right; }
  td.long { white-space: normal; word-break: break-word; max-width: 720px; }
  .rule { margin-top: 24px; }
  .rule .meta { color: var(--ink3); font-family: var(--mono); font-size: 12px; }
  .empty { color: var(--ink3); font-style: italic; }
</style>
</head>
<body>
<main>
  <h1>Validate report</h1>
  <p class="sub">{{fmtTime .StartedAt}} → {{fmtTime .FinishedAt}}
     · {{elapsed .StartedAt .FinishedAt}}
     · period {{.Period}}</p>

  <h2>Summary</h2>
  <div class="panel"><dl class="stats">
    <dt>warnings</dt><dd class="warn">{{.WarningsTotal}}</dd>
    <dt>errors</dt><dd class="{{if gt .ErrorsTotal 0}}fail{{else}}ok{{end}}">{{.ErrorsTotal}}</dd>
  </dl></div>

  <h2>Rules</h2>
  <table>
    <tr><th>id</th><th>name</th><th class="num">scanned</th>
        <th class="num">warn</th><th class="num">error</th><th class="num">elapsed</th></tr>
    {{range .PerRule}}
    <tr>
      <td>{{.ID}}</td>
      <td>{{.Name}}</td>
      <td class="num">{{.Scanned}}</td>
      <td class="num warn">{{.Warnings}}</td>
      <td class="num {{if gt .Errors 0}}fail{{end}}">{{.Errors}}</td>
      <td class="num">{{ms .Elapsed}}</td>
    </tr>
    {{end}}
  </table>

  {{range .PerRule}}
  <div class="rule">
    <h3>{{.ID}} — {{.Name}}</h3>
    <p class="meta">scanned {{.Scanned}} · {{.Warnings}} warning(s), {{.Errors}} error(s) · {{ms .Elapsed}}</p>
    {{if .Findings}}
    <table>
      <tr><th>severity</th><th>station_id</th><th>issue</th></tr>
      {{range .Findings}}
      <tr>
        <td class="{{if eq .Severity "error"}}fail{{else}}warn{{end}}">{{.Severity}}</td>
        <td>{{if .StationID}}{{.StationID}}{{else}}—{{end}}</td>
        <td class="long">{{.Issue}}</td>
      </tr>
      {{end}}
    </table>
    {{else}}
    <p class="empty">No findings.</p>
    {{end}}
  </div>
  {{end}}
</main>
</body>
</html>
`
