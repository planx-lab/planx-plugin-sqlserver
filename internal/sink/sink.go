// Package sink implements the sqlserver-sink component: prepared INSERT inside
// a transaction (design §5). It writes through a dbExecutor seam so unit tests
// need no real SQL Server (fakeDB records the INSERT + args; no go-mssqldb /
// sql.Open in _test.go).
package sink

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/planx-lab/planx-plugin-sqlserver/internal/dbbatch"
	"github.com/planx-lab/planx-sdk-go/sdk"
)

// Config configures the sqlserver-sink component (design §7 ConfigSchema).
type Config struct {
	Host      string `json:"host"`
	Port      int    `json:"port"`
	Database  string `json:"database"`
	User      string `json:"user"`
	Password  string `json:"password"`
	Table     string `json:"table"`
	Columns   string `json:"columns"`   // comma-separated; if empty, uses batch column schema
	BatchRows int    `json:"batchRows"` // rows per INSERT transaction (unused for read path; informational)
	Encrypt   string `json:"encrypt"`
}

const (
	defaultPort      = 1433
	defaultBatchRows = 1000
	defaultEncrypt   = "false"
)

// Sink prepared-INSERTs DBBatch rows in a single transaction per batch (design
// §5). The prepared-INSERT-in-tx path is chosen over mssql.CopyIn/BulkCopy
// because the latter forces importing the mssql subpackage, requires a fixed
// known column list, and breaks Always Encrypted (design §5 rationale).
type Sink struct {
	cfg Config
	db  dbExecutor
}

// New returns a zero-value Sink satisfying sdk.SinkSPI.
func New() sdk.SinkSPI { return &Sink{} }

func init() {
	// dbbatch types must be gob-registered on BOTH the source and sink
	// processes for the SDK's gob-through-interface codec to round-trip
	// (design §3). Exactly two calls; the top-level type is concrete. The
	// source registers them too; double-registration is a no-op.
	sdk.RegisterType(dbbatch.DBBatch{})
	sdk.RegisterType(dbbatch.DBRow{})
}

// Init parses config, validates required fields, applies defaults, builds the
// DSN, connects (sql.Open), and pings. The connection handle is stored as a
// dbExecutor seam so WriteBatch and tests go through the same interface.
func (s *Sink) Init(ctx context.Context, cfg []byte) error {
	if err := parseConfig(string(cfg), &s.cfg); err != nil {
		return err
	}
	if err := validateConfig(&s.cfg); err != nil {
		return err
	}
	applyDefaults(&s.cfg)

	dsn := buildDSN(s.cfg)
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return fmt.Errorf("sqlserver sink: connect: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return fmt.Errorf("sqlserver sink: ping: %w", err)
	}
	s.db = &mssqlDB{db: db}
	return nil
}

// parseConfig unmarshals the JSON config, wrapping parse errors with the
// connector/component prefix (design: "<connector> sink: config: %w").
func parseConfig(raw string, cfg *Config) error {
	if err := json.Unmarshal([]byte(raw), cfg); err != nil {
		return fmt.Errorf("sqlserver sink: config: %w", err)
	}
	return nil
}

// validateConfig enforces the required fields from design §7.
func validateConfig(cfg *Config) error {
	if cfg.Host == "" {
		return fmt.Errorf("sqlserver sink: host is required")
	}
	if cfg.Database == "" {
		return fmt.Errorf("sqlserver sink: database is required")
	}
	if cfg.User == "" {
		return fmt.Errorf("sqlserver sink: user is required")
	}
	if cfg.Password == "" {
		return fmt.Errorf("sqlserver sink: password is required")
	}
	if cfg.Table == "" {
		return fmt.Errorf("sqlserver sink: table is required")
	}
	return nil
}

// applyDefaults fills port/batchRows/encrypt when unset (design §7).
func applyDefaults(cfg *Config) {
	if cfg.Port == 0 {
		cfg.Port = defaultPort
	}
	if cfg.BatchRows <= 0 {
		cfg.BatchRows = defaultBatchRows
	}
	if cfg.Encrypt == "" {
		cfg.Encrypt = defaultEncrypt
	}
}

// buildDSN constructs the mssqldb connection URL via net/url so the password is
// URL-encoded safely (url.UserPassword handles special chars). The password is
// never fmt.Printf-ed by this package. Identical to the source's buildDSN.
func buildDSN(cfg Config) string {
	u := url.URL{
		Scheme: "sqlserver",
		Host:   fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
	}
	if cfg.User != "" {
		u.User = url.UserPassword(cfg.User, cfg.Password)
	}
	q := u.Query()
	q.Set("database", cfg.Database)
	q.Set("encrypt", cfg.Encrypt)
	u.RawQuery = q.Encode()
	return u.String()
}

// parseColumns splits a comma-separated column list, trimming whitespace. Empty
// fields are dropped (design §5 columns override). Returns nil for the empty
// string so the caller can fall back to batch.Columns.
func parseColumns(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	cols := make([]string, 0, len(parts))
	for _, p := range parts {
		if c := strings.TrimSpace(p); c != "" {
			cols = append(cols, c)
		}
	}
	return cols
}

// mssqlPlaceholders builds the @p1,@p2,... placeholder list for n columns
// (design §5 — mssql uses @pN named params, not ?/$N).
func mssqlPlaceholders(n int) string {
	ps := make([]string, n)
	for i := 0; i < n; i++ {
		ps[i] = fmt.Sprintf("@p%d", i+1)
	}
	return strings.Join(ps, ",")
}

// WriteBatch prepared-INSERTs a DBBatch inside a single transaction (design §5):
//   - type-assert to dbbatch.DBBatch (clear error on mismatch);
//   - empty batch is a no-op — BeginTx is NOT called;
//   - columns: config override wins, else batch.Columns;
//   - build INSERT INTO <table> (cols) VALUES (@p1,...) once and Prepare it;
//   - per row, decode via dbbatch.DecodeRowToArgs (typed args for correct
//     INSERT — int64/time.Time/[]byte/nil, NULL -> nil not empty string) and
//     ExecContext the prepared statement;
//   - on any DB error, rollback and return the wrapped error.
//
// This is append-only — no upsert/MERGE (design §5 v1 decision).
func (s *Sink) WriteBatch(batch sdk.Batch) error {
	dbb, ok := batch.(dbbatch.DBBatch)
	if !ok {
		return fmt.Errorf("sqlserver sink: expected dbbatch.DBBatch, got %T", batch)
	}
	if len(dbb.Rows) == 0 {
		return nil
	}

	cols := dbb.Columns
	if override := parseColumns(s.cfg.Columns); len(override) > 0 {
		cols = override
	}

	stmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		s.cfg.Table, strings.Join(cols, ","), mssqlPlaceholders(len(cols)))

	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sqlserver sink: begin: %w", err)
	}
	prepared, err := tx.PrepareContext(ctx, stmt)
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("sqlserver sink: prepare: %w", err)
	}
	for _, row := range dbb.Rows {
		args, err := dbbatch.DecodeRowToArgs(row)
		if err != nil {
			prepared.Close()
			_ = tx.Rollback()
			return fmt.Errorf("sqlserver sink: decode: %w", err)
		}
		if _, err := prepared.ExecContext(ctx, args...); err != nil {
			prepared.Close()
			_ = tx.Rollback()
			return fmt.Errorf("sqlserver sink: exec: %w", err)
		}
	}
	if err := prepared.Close(); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("sqlserver sink: close stmt: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sqlserver sink: commit: %w", err)
	}
	return nil
}

// Close releases the connection. Idempotent: Close on an uninit Sink (no db)
// returns nil (matches csv / source).
func (s *Sink) Close() error {
	if s.db != nil {
		s.db.Close()
	}
	return nil
}
