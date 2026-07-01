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
type Config struct {
	Host      string `json:"host"`
	Port      int    `json:"port"`
	Database  string `json:"database"`
	User      string `json:"user"`
	Password  string `json:"password"`
	Query     string `json:"query"`
	BatchRows int    `json:"batchRows"`
	Encrypt   string `json:"encrypt"`
}

const (
	defaultPort      = 1433
	defaultBatchRows = 1000
	defaultEncrypt   = "false"
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

func init() {
	// dbbatch types must be gob-registered on BOTH the source and sink
	// processes for the SDK's gob-through-interface codec to round-trip
	// (design §3). Exactly two calls; the top-level type is concrete.
	sdk.RegisterType(dbbatch.DBBatch{})
	sdk.RegisterType(dbbatch.DBRow{})
}

// Init parses config, validates required fields, applies defaults, builds the
// DSN, connects (sql.Open), pings, and issues the SELECT to obtain a rows cursor.
func (s *Source) Init(ctx context.Context, cfg []byte) error {
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
		return fmt.Errorf("sqlserver source: connect: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return fmt.Errorf("sqlserver source: ping: %w", err)
	}
	s.q = &mssqlQuerier{db: db}

	rows, err := s.q.Query(ctx, s.cfg.Query)
	if err != nil {
		s.q.Close()
		return fmt.Errorf("sqlserver source: query: %w", err)
	}
	s.rows = rows
	s.columns = rows.Columns()
	return nil
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
	if cfg.Query == "" {
		return fmt.Errorf("sqlserver source: query is required")
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
