// Package source implements the sqlserver-source component: a batch SELECT
// reader that emits dbbatch.DBBatch payloads (design §4). It reads rows through
// a querier seam so unit tests need no real SQL Server.
package source

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/url"

	"github.com/planx-lab/planx-plugin-sqlserver/internal/dbbatch"
	"github.com/planx-lab/planx-sdk-go/sdk"
)

// Config configures the sqlserver-source component (design §4 ConfigSchema).
// The raw-SQL `query` field was replaced (ADR-013) by `table`+`columns`: the
// Source now builds `SELECT {cols} FROM {table}` internally so no user-supplied
// SQL reaches the DB. DiscoverSchema introspects information_schema to guide
// table/column selection in the Designer.
type Config struct {
	Host      string `json:"host"`
	Port      int    `json:"port"`
	Database  string `json:"database"`
	User      string `json:"user"`
	Password  string `json:"password"`
	Table     string `json:"table"`
	Columns   string `json:"columns"`
	BatchRows int    `json:"batchRows"`
	Encrypt   string `json:"encrypt"`
}

const (
	defaultPort      = 1433
	defaultBatchRows = 1000
	defaultEncrypt   = "disable"
)

// Source reads a finite SELECT result set in row batches. EOF terminates the
// stream so the DAG runtime reaches SUCCEEDED.
type Source struct {
	cfg     Config
	q       querier
	rows    rowsIterator
	columns []string
	done    bool
}

// New returns a zero-value Source satisfying sdk.SourceSPI.
func New() sdk.SourceSPI { return &Source{} }

// dbbatch gob registration is centralized in the dbbatch package's init()
// (gob.RegisterName under a shared wire name for cross-connector interop).

// Init parses config, validates required fields, applies defaults, connects
// (sql.Open), and issues the SELECT — built internally from table+columns
// (ADR-013) — to obtain a rows cursor. No user-supplied SQL reaches the DB.
func (s *Source) Init(ctx context.Context, cfg []byte) error {
	if err := parseConfig(string(cfg), &s.cfg); err != nil {
		return err
	}
	if err := validateConfig(&s.cfg); err != nil {
		return err
	}
	applyDefaults(&s.cfg)

	q, err := ConnectQuerier(ctx, s.cfg)
	if err != nil {
		return err
	}
	s.q = q

	cols := s.cfg.Columns
	if cols == "" {
		cols = "*"
	}
	query := fmt.Sprintf("SELECT %s FROM %s", cols, s.cfg.Table)
	rows, err := s.q.Query(context.Background(), query)
	if err != nil {
		s.q.Close()
		return fmt.Errorf("sqlserver source: query: %w", err)
	}
	s.rows = rows
	s.columns = rows.Columns()
	return nil
}

// ConnectQuerier builds the DSN, opens a *sql.DB, and pings. Extracted from
// Init so DiscoverSchema can reuse the same connect path against a temporary
// connection (opened, queried, closed) without constructing a Source.
func ConnectQuerier(ctx context.Context, cfg Config) (Querier, error) {
	dsn := buildDSN(cfg)
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, fmt.Errorf("sqlserver source: connect: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("sqlserver source: ping: %w", err)
	}
	return &mssqlQuerier{db: db}, nil
}

// parseConfig unmarshals the JSON config, wrapping parse errors with the
// connector/component prefix (design: "<connector> source: config: %w").
func parseConfig(raw string, cfg *Config) error {
	if err := json.Unmarshal([]byte(raw), cfg); err != nil {
		return fmt.Errorf("sqlserver source: config: %w", err)
	}
	return nil
}

// validateConfig enforces the required fields from design §4.
func validateConfig(cfg *Config) error {
	if cfg.Host == "" {
		return fmt.Errorf("sqlserver source: host is required")
	}
	if cfg.Database == "" {
		return fmt.Errorf("sqlserver source: database is required")
	}
	if cfg.User == "" {
		return fmt.Errorf("sqlserver source: user is required")
	}
	if cfg.Password == "" {
		return fmt.Errorf("sqlserver source: password is required")
	}
	if cfg.Table == "" {
		return fmt.Errorf("sqlserver source: table is required")
	}
	return nil
}

// applyDefaults fills port/batchRows/encrypt when unset (design §4).
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
// never fmt.Printf-ed by this package.
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

// ReadBatch reads up to BatchRows records and returns them as a dbbatch.DBBatch.
// Two-phase EOF (design §4) — MANDATORY:
//   - a partial trailing batch is returned first with nil error, THEN io.EOF on
//     the next call. Returning io.EOF while rows are buffered drops data.
//   - an empty result yields io.EOF immediately.
//   - after the rows.Next() loop, rows.Err() is checked to surface delayed
//     driver errors (database/sql surfaces them there, not from Next).
func (s *Source) ReadBatch() (sdk.Batch, error) {
	batch := make([]dbbatch.DBRow, 0, s.cfg.BatchRows)
	for len(batch) < s.cfg.BatchRows {
		if !s.rows.Next() {
			s.done = true
			break
		}
		vals, err := s.rows.ScanValues()
		if err != nil {
			return nil, fmt.Errorf("sqlserver source: scan: %w", err)
		}
		row, err := dbbatch.EncodeRow(vals)
		if err != nil {
			return nil, fmt.Errorf("sqlserver source: scan: %w", err)
		}
		batch = append(batch, row)
	}
	// Delayed driver errors land here, not on Next().
	if err := s.rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlserver source: read: %w", err)
	}
	if s.done && len(batch) == 0 {
		return nil, io.EOF
	}
	return dbbatch.DBBatch{Columns: s.columns, Rows: batch}, nil
}

// Close releases the rows cursor and the connection. Idempotent: Close on an
// uninit Source (no querier) returns nil (matches csv).
func (s *Source) Close() error {
	if s.rows != nil {
		s.rows.Close()
	}
	if s.q != nil {
		s.q.Close()
	}
	return nil
}
