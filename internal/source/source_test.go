package source

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/planx-lab/planx-plugin-sqlserver/internal/dbbatch"
	"github.com/planx-lab/planx-sdk-go/sdk"
)

// --- fakeQuerier: the DB-free seam (design §8) -------------------------------
//
// Injects canned rows so unit tests never touch a real SQL Server. Lives in the
// same package so it can satisfy the unexported querier/rowsIterator interfaces.
// grep mssqldb / sql.Open in _test.go -> nothing (real DB never reached).
//
// The seam is ScanValues() (NOT pgx's Values()): mssql scans into *interface{}
// (new(any)) ptrs -> rows.Scan -> deref. The fake short-circuits this and yields
// already-deref'd []any, which is exactly what the production mssqlRows returns
// from ScanValues after dereferencing.

// fakeRows is a rowsIterator over a fixed slice of value-rows.
type fakeRows struct {
	cols []string
	data [][]any // one inner slice per row, parallel to cols (the deref'd values)
	i    int
	err  error // surfaced via Err() once exhausted
}

func (r *fakeRows) Next() bool {
	if r.i < len(r.data) {
		r.i++
		return true
	}
	return false
}
func (r *fakeRows) Columns() []string { return r.cols }
func (r *fakeRows) ScanValues() ([]any, error) {
	return r.data[r.i-1], nil
}
func (r *fakeRows) Err() error { return r.err }
func (r *fakeRows) Close()     {}

// fakeQuerier is a querier that returns a single, pre-built rowsIterator.
type fakeQuerier struct {
	rows *fakeRows
}

func (q *fakeQuerier) Query(_ context.Context, _ string, _ ...any) (rowsIterator, error) {
	return q.rows, nil
}
func (q *fakeQuerier) Close() {}

// newTestSource wires a Source with config already parsed + a fake querier.
// Skips the real DSN/connect path (no sql.Open, no Ping): sets s.rows directly
// from the fake so ReadBatch exercises the seam without a connection.
func newTestSource(t *testing.T, cfg Config, q querier) *Source {
	t.Helper()
	fr := q.(*fakeQuerier).rows
	s := &Source{cfg: cfg, q: q, rows: fr, columns: fr.cols}
	return s
}

// =============================================================================
// 1. Compile-time SPI conformance
// =============================================================================

func TestSource_New_ReturnsSourceSPI(t *testing.T) {
	var _ sdk.SourceSPI = New()
}

// =============================================================================
// 2. Init config-parse
// =============================================================================

func TestSource_Init_ParsesValidConfig(t *testing.T) {
	// Init attempts a real connection; we only assert config parsing succeeds.
	// To keep this DB-free, we unit-test the parse via a private helper.
	cfg := Config{}
	raw := `{"host":"db","port":1433,"database":"shop","user":"u","password":"p","query":"SELECT 1","batchRows":50,"encrypt":"true"}`
	if err := parseConfig(raw, &cfg); err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.Host != "db" || cfg.Port != 1433 || cfg.Database != "shop" ||
		cfg.User != "u" || cfg.Password != "p" || cfg.Query != "SELECT 1" ||
		cfg.BatchRows != 50 || cfg.Encrypt != "true" {
		t.Fatalf("parsed config mismatch: %+v", cfg)
	}
}

func TestSource_Init_InvalidJSON_WrappedError(t *testing.T) {
	var cfg Config
	err := parseConfig(`{not json`, &cfg)
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
	// Must be wrapped with the "sqlserver source: config:" prefix.
	if !contains(err.Error(), "sqlserver source: config:") {
		t.Fatalf("error prefix: got %q", err.Error())
	}
}

func TestSource_Init_MissingRequired(t *testing.T) {
	cases := []struct {
		name string
		json string
		want string // substring of the missing field
	}{
		{"host", `{"database":"d","user":"u","password":"p","query":"q"}`, "host"},
		{"database", `{"host":"h","user":"u","password":"p","query":"q"}`, "database"},
		{"user", `{"host":"h","database":"d","password":"p","query":"q"}`, "user"},
		{"password", `{"host":"h","database":"d","user":"u","query":"q"}`, "password"},
		{"query", `{"host":"h","database":"d","user":"u","password":"p"}`, "query"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var cfg Config
			if err := parseConfig(c.json, &cfg); err != nil {
				t.Fatalf("parseConfig: %v", err)
			}
			if err := validateConfig(&cfg); err == nil {
				t.Fatalf("expected error for missing %s", c.want)
			} else if !contains(err.Error(), c.want) {
				t.Fatalf("missing %s: got %q", c.want, err.Error())
			}
		})
	}
}

func TestSource_Init_DefaultsApplied(t *testing.T) {
	var cfg Config
	raw := `{"host":"h","database":"d","user":"u","password":"p","query":"q"}`
	if err := parseConfig(raw, &cfg); err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if err := validateConfig(&cfg); err != nil {
		t.Fatalf("validateConfig: %v", err)
	}
	applyDefaults(&cfg)
	if cfg.Port != 1433 {
		t.Errorf("port default: got %d, want 1433", cfg.Port)
	}
	if cfg.BatchRows != 1000 {
		t.Errorf("batchRows default: got %d, want 1000", cfg.BatchRows)
	}
	if cfg.Encrypt != "disable" {
		t.Errorf("encrypt default: got %q, want %q", cfg.Encrypt, "disable")
	}
}

// =============================================================================
// 3. ReadBatch via fakeQuerier
// =============================================================================

func TestSource_ReadBatch_FullBatch(t *testing.T) {
	q := &fakeQuerier{rows: &fakeRows{
		cols: []string{"id", "name"},
		data: [][]any{
			{int64(1), "alice"},
			{int64(2), "bob"},
		},
	}}
	s := newTestSource(t, Config{BatchRows: 2}, q)

	b, err := s.ReadBatch()
	if err != nil {
		t.Fatalf("ReadBatch: %v", err)
	}
	dbb, ok := b.(dbbatch.DBBatch)
	if !ok {
		t.Fatalf("batch type: %T", b)
	}
	if len(dbb.Rows) != 2 {
		t.Fatalf("rows: got %d, want 2", len(dbb.Rows))
	}
}

// TestSource_ReadBatch_PartialThenEOF asserts the CRITICAL two-phase EOF:
// a partial trailing batch is returned first (nil error), THEN io.EOF on the
// next call. Returning io.EOF while rows are buffered drops data (forbidden).
func TestSource_ReadBatch_PartialThenEOF(t *testing.T) {
	q := &fakeQuerier{rows: &fakeRows{
		cols: []string{"id"},
		data: [][]any{{int64(1)}, {int64(2)}, {int64(3)}},
	}}
	s := newTestSource(t, Config{BatchRows: 2}, q)

	// Call 1: full batch of 2.
	b1, err := s.ReadBatch()
	if err != nil {
		t.Fatalf("ReadBatch 1: %v", err)
	}
	if len(b1.(dbbatch.DBBatch).Rows) != 2 {
		t.Fatalf("batch 1 rows: got %d, want 2", len(b1.(dbbatch.DBBatch).Rows))
	}

	// Call 2: PARTIAL trailing batch of 1, nil error (NOT io.EOF — rows exist).
	b2, err := s.ReadBatch()
	if err != nil {
		t.Fatalf("ReadBatch 2 (partial): %v — must return the partial batch with nil error", err)
	}
	if got := len(b2.(dbbatch.DBBatch).Rows); got != 1 {
		t.Fatalf("batch 2 rows: got %d, want 1 (the partial remainder)", got)
	}

	// Call 3: NOW io.EOF — stream genuinely exhausted.
	_, err = s.ReadBatch()
	if !errors.Is(err, io.EOF) {
		t.Fatalf("ReadBatch 3: got %v, want io.EOF", err)
	}
}

func TestSource_ReadBatch_EmptyResult_ImmediateEOF(t *testing.T) {
	q := &fakeQuerier{rows: &fakeRows{
		cols: []string{"id"},
		data: nil,
	}}
	s := newTestSource(t, Config{BatchRows: 10}, q)

	_, err := s.ReadBatch()
	if !errors.Is(err, io.EOF) {
		t.Fatalf("empty result: got %v, want io.EOF", err)
	}
}

func TestSource_ReadBatch_RowsErr_Surfaced(t *testing.T) {
	rowErr := errors.New("connection reset")
	q := &fakeQuerier{rows: &fakeRows{
		cols: []string{"id"},
		data: [][]any{{int64(1)}},
		err: rowErr,
	}}
	s := newTestSource(t, Config{BatchRows: 10}, q)

	_, err := s.ReadBatch()
	if err == nil {
		t.Fatal("expected error from rows.Err()")
	}
	if !errors.Is(err, rowErr) {
		t.Fatalf("rows.Err not surfaced: got %q", err.Error())
	}
}

// =============================================================================
// 4. scanRow fidelity — every Kind tag via dbbatch.EncodeRow
// =============================================================================

func TestSource_ReadBatch_TypeFidelity(t *testing.T) {
	ts := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	q := &fakeQuerier{rows: &fakeRows{
		cols: []string{"i", "f", "s", "b", "t", "by", "n"},
		data: [][]any{{
			int64(42),
			float64(3.5),
			"hello",
			true,
			ts,
			[]byte{0xDE, 0xAD, 0xBE, 0xEF},
			nil,
		}},
	}}
	s := newTestSource(t, Config{BatchRows: 10}, q)

	b, err := s.ReadBatch()
	if err != nil {
		t.Fatalf("ReadBatch: %v", err)
	}
	dbb := b.(dbbatch.DBBatch)
	row := dbb.Rows[0]

	// Kind tags per dbbatch constants.
	wantTypes := []byte{
		dbbatch.KindInt, dbbatch.KindFloat, dbbatch.KindString,
		dbbatch.KindBool, dbbatch.KindTime, dbbatch.KindBytes, dbbatch.KindNil,
	}
	for i, want := range wantTypes {
		if row.Types[i] != want {
			t.Errorf("col %d kind: got %d, want %d", i, row.Types[i], want)
		}
	}

	// Spot-check value encodings that dbbatch.EncodeRow produces.
	if row.Vals[0] != "42" {
		t.Errorf("int val: got %q", row.Vals[0])
	}
	if row.Vals[1] != "3.5" {
		t.Errorf("float val: got %q", row.Vals[1])
	}
	if row.Vals[3] != "true" {
		t.Errorf("bool val: got %q", row.Vals[3])
	}
	if row.Vals[4] != ts.Format(time.RFC3339Nano) {
		t.Errorf("time val: got %q", row.Vals[4])
	}
	if row.Vals[6] != "" {
		t.Errorf("nil val: got %q, want empty", row.Vals[6])
	}
}

// =============================================================================
// 5. Close
// =============================================================================

func TestSource_Close_Uninit_NilError(t *testing.T) {
	s := New()
	if err := s.Close(); err != nil {
		t.Fatalf("Close uninit: %v", err)
	}
}

func TestSource_Close_AfterInit_CallsQuerierClose(t *testing.T) {
	q := &closeTrackingQuerier{}
	s := &Source{q: q}
	if err := s.Close(); err != nil {
		t.Fatalf("Close after init: %v", err)
	}
	if !q.closed {
		t.Fatal("querier.Close was not called")
	}
}

// closeTrackingQuerier records Close invocations without running a real query.
type closeTrackingQuerier struct {
	closed bool
}

func (q *closeTrackingQuerier) Query(_ context.Context, _ string, _ ...any) (rowsIterator, error) {
	return &fakeRows{}, nil
}
func (q *closeTrackingQuerier) Close() { q.closed = true }

// =============================================================================
// helpers
// =============================================================================

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
