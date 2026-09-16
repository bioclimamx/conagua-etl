package validate_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// recordingDriver wraps the registered sqlite driver so that every
// statement a connection prepares is captured by text. database/sql
// routes a query through Prepare whenever the connection offers neither
// QueryerContext nor ExecerContext, and recordingConn deliberately
// promotes only driver.Conn's three methods, so the capture is complete:
// a rule cannot run a statement this recorder does not see.
type recordingDriver struct {
	inner driver.Driver
	mu    sync.Mutex
	stmts []string
}

func (d *recordingDriver) Open(name string) (driver.Conn, error) {
	c, err := d.inner.Open(name)
	if err != nil {
		return nil, err
	}
	return &recordingConn{Conn: c, d: d}, nil
}

func (d *recordingDriver) record(query string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.stmts = append(d.stmts, query)
}

// reset clears the capture; statements returns what was captured since.
func (d *recordingDriver) reset() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.stmts = nil
}

func (d *recordingDriver) statements() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.stmts...)
}

type recordingConn struct {
	driver.Conn
	d *recordingDriver
}

func (c *recordingConn) Prepare(query string) (driver.Stmt, error) {
	c.d.record(query)
	return c.Conn.Prepare(query)
}

func (c *recordingConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	c.d.record(query)
	if p, ok := c.Conn.(driver.ConnPrepareContext); ok {
		return p.PrepareContext(ctx, query)
	}
	return c.Conn.Prepare(query)
}

const recordingDriverName = "recording-sqlite"

var (
	recording    = &recordingDriver{}
	registerOnce sync.Once
)

// openRecording opens the database at path through the recording
// driver — a single connection, so a statement sequence is one
// connection's — registering the driver on first use and scheduling the
// close. The file must already exist with its schema applied (a
// schema.Open handle closed beforehand), since nothing here applies DDL.
func openRecording(path string) *sql.DB {
	GinkgoHelper()
	registerOnce.Do(func() {
		probe, err := sql.Open("sqlite", path)
		Expect(err).NotTo(HaveOccurred())
		recording.inner = probe.Driver()
		Expect(probe.Close()).To(Succeed())
		sql.Register(recordingDriverName, recording)
	})
	db, err := sql.Open(recordingDriverName, path)
	Expect(err).NotTo(HaveOccurred())
	db.SetMaxOpenConns(1)
	DeferCleanup(func() { _ = db.Close() })
	return db
}
