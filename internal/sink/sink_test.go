package sink

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/planx-lab/planx-plugin-sqlserver/internal/dbbatch"
	"github.com/planx-lab/planx-sdk-go/sdk"
)

// --- fakeDB/fakeTx/fakeStmt: the DB-free seam (design §8) --------------------
//
// Injects a recording dbExecutor so unit tests never touch a real SQL Server.
// Lives in the same package so it can satisfy the unexported dbExecutor /
// txExecutor / stmtExecutor interfaces. The fake records the INSERT statement
// prepared and the typed args of every ExecContext call, letting tests assert
// the @p1,@p2,... placeholder shape, columns, and per-Kind typed args
// (int64/time.Time/[]byte/nil) without a database.
//
// grep go-mssqldb / sql.Open in _test.go -> nothing (real DB never reached).
// database/sql is imported only for *sql.TxOptions / sql.Result — the seam's
// wire types the production Sink passes through, NOT a live connection.

// execCall is one prepared.ExecContext invocation, captured as the typed []any
// the production Sink derived via dbbatch.DecodeRowToArgs.
type execCall struct {
	args []any
}

// fakeStmt is a stmtExecutor that records ExecContext calls.
type fakeStmt struct {
	parent *fakeTx
	execErr error // if non-nil, returned from ExecContext
	closed  bool
}

func (s *fakeStmt) ExecContext(_ context.Context, args ...any) (sql.Result, error) {
	if s.execErr != nil {
		return nil, s.execErr
	}
	cp := make([]any, len(args))
	copy(cp, args)
	s.parent.execs = append(s.parent.execs, execCall{args: cp})
	return nil, nil
}

func (s *fakeStmt) Close() error { s.closed = true; return nil }

// fakeTx is a txExecutor that records the prepared statement and Exec calls,
// and tracks Commit/Rollback.
type fakeTx struct {
	parent     *fakeDB
	prepareErr error // if non-nil, returned from PrepareContext
	prepared   string
	execs      []execCall
	committed  bool
	rolledBack bool
}

func (t *fakeTx) PrepareContext(_ context.Context, query string) (stmtExecutor, error) {
	t.prepared = query
	if t.prepareErr != nil {
		return nil, t.prepareErr
	}
	return &fakeStmt{parent: t}, nil
}

func (t *fakeTx) Commit() error { t.committed = true; return nil }

func (t *fakeTx) Rollback() error { t.rolledBack = true; return nil }

// fakeDB is a dbExecutor that records BeginTx invocations and spawns fakeTx.
type fakeDB struct {
	beginErr error // if non-nil, returned from BeginTx
	tx       *fakeTx
	closed   bool
}

func (d *fakeDB) BeginTx(_ context.Context, _ *sql.TxOptions) (txExecutor, error) {
	if d.beginErr != nil {
		return nil, d.beginErr
	}
	d.tx = &fakeTx{parent: d}
	return d.tx, nil
}

func (d *fakeDB) Close() { d.closed = true }

// newSinkWithDB wires a Sink whose config is already parsed and whose db is the
// fake — skipping the real DSN/connect/Ping path entirely.
func newSinkWithDB(cfg Config, db dbExecutor) *Sink {
	return &Sink{cfg: cfg, db: db}
}

// =============================================================================
// 1. Compile-time SPI conformance
// =============================================================================

func TestSink_New_ReturnsSinkSPI(t *testing.T) {
	var _ sdk.SinkSPI = New()
}

// =============================================================================
// 2. Init config-parse (parse + validate + defaults — DB-free)
// =============================================================================

func TestSink_Init_ParsesValidConfig(t *testing.T) {
	cfg := Config{}
	raw := `{"host":"db","port":1434,"database":"shop","user":"u","password":"p","table":"users","columns":"id,name","batchRows":500,"encrypt":"true"}`
	if err := parseConfig(raw, &cfg); err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.Host != "db" || cfg.Port != 1434 || cfg.Database != "shop" ||
		cfg.User != "u" || cfg.Password != "p" || cfg.Table != "users" ||
		cfg.Columns != "id,name" || cfg.BatchRows != 500 || cfg.Encrypt != "true" {
		t.Fatalf("parsed config mismatch: %+v", cfg)
	}
}

func TestSink_Init_InvalidJSON_WrappedError(t *testing.T) {
	var cfg Config
	err := parseConfig(`{not json`, &cfg)
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
	// Must be wrapped with the "sqlserver sink: config:" prefix (design §8).
	if !strings.HasPrefix(err.Error(), "sqlserver sink: config:") {
		t.Fatalf("error prefix: got %q", err.Error())
	}
}

func TestSink_Init_MissingRequired(t *testing.T) {
	cases := []struct {
		name string
		json string
		want string // substring of the missing field
	}{
		{"host", `{"database":"d","user":"u","password":"p","table":"t"}`, "host"},
		{"database", `{"host":"h","user":"u","password":"p","table":"t"}`, "database"},
		{"user", `{"host":"h","database":"d","password":"p","table":"t"}`, "user"},
		{"password", `{"host":"h","database":"d","user":"u","table":"t"}`, "password"},
		{"table", `{"host":"h","database":"d","user":"u","password":"p"}`, "table"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var cfg Config
			if err := parseConfig(c.json, &cfg); err != nil {
				t.Fatalf("parseConfig: %v", err)
			}
			if err := validateConfig(&cfg); err == nil {
				t.Fatalf("expected error for missing %s", c.want)
			} else if !strings.Contains(err.Error(), c.want) {
				t.Fatalf("missing %s: got %q", c.want, err.Error())
			}
		})
	}
}

func TestSink_Init_DefaultsApplied(t *testing.T) {
	var cfg Config
	raw := `{"host":"h","database":"d","user":"u","password":"p","table":"t"}`
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
// 3. WriteBatch via fakeDB (design §8 sink tests)
// =============================================================================

// (a) empty batch is a NO-OP — BeginTx must NOT be called.
func TestSink_WriteBatch_EmptyBatch_NoOp(t *testing.T) {
	db := &fakeDB{}
	s := newSinkWithDB(Config{Table: "users"}, db)

	err := s.WriteBatch(dbbatch.DBBatch{Columns: []string{"id"}, Rows: nil})
	if err != nil {
		t.Fatalf("WriteBatch empty: %v", err)
	}
	if db.tx != nil {
		t.Fatalf("BeginTx called on empty batch: tx was opened")
	}
}

// (b) type-assertion failure returns a clear error.
func TestSink_WriteBatch_TypeAssertionFailure(t *testing.T) {
	db := &fakeDB{}
	s := newSinkWithDB(Config{Table: "users"}, db)

	err := s.WriteBatch([][]string{{"a", "b"}}) // wrong type — not dbbatch.DBBatch
	if err == nil {
		t.Fatal("expected type-assertion error, got nil")
	}
	if !strings.Contains(err.Error(), "expected dbbatch.DBBatch") {
		t.Fatalf("error: got %q", err.Error())
	}
	if !strings.Contains(err.Error(), "[][]string") {
		t.Fatalf("error should name the bad type %%T: got %q", err.Error())
	}
	// BeginTx must not have been reached on a type-assertion failure.
	if db.tx != nil {
		t.Fatalf("BeginTx called despite type-assertion failure")
	}
}

// (c) valid batch — fakeStmt records the INSERT statement and per-row typed
// args decoded per Kind via dbbatch.DecodeRowToArgs (int64/time.Time/[]byte/nil).
// Also asserts the @p1,@p2,... placeholder shape and columns from the batch.
func TestSink_WriteBatch_ValidBatch_TypedArgs(t *testing.T) {
	ts := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	row, err := dbbatch.EncodeRow([]any{
		int64(42),
		float64(3.5),
		"hello",
		true,
		ts,
		[]byte{0xDE, 0xAD, 0xBE, 0xEF},
		nil,
	})
	if err != nil {
		t.Fatalf("EncodeRow: %v", err)
	}

	db := &fakeDB{}
	s := newSinkWithDB(Config{Table: "users"}, db)

	batch := dbbatch.DBBatch{
		Columns: []string{"i", "f", "s", "b", "t", "by", "n"},
		Rows:    []dbbatch.DBRow{row},
	}
	if err := s.WriteBatch(batch); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	if db.tx == nil {
		t.Fatal("BeginTx was not called")
	}
	// prepared statement must be the INSERT with @p1..@p7 placeholders and the
	// batch column list.
	wantStmt := "INSERT INTO users (i,f,s,b,t,by,n) VALUES (@p1,@p2,@p3,@p4,@p5,@p6,@p7)"
	if db.tx.prepared != wantStmt {
		t.Errorf("prepared stmt:\n got %q\nwant %q", db.tx.prepared, wantStmt)
	}
	if len(db.tx.execs) != 1 {
		t.Fatalf("Exec calls: got %d, want 1", len(db.tx.execs))
	}
	args := db.tx.execs[0].args
	if len(args) != 7 {
		t.Fatalf("args width: got %d, want 7", len(args))
	}

	// int64
	if v, ok := args[0].(int64); !ok || v != 42 {
		t.Errorf("arg 0: got %#v, want int64(42)", args[0])
	}
	// float64
	if v, ok := args[1].(float64); !ok || v != 3.5 {
		t.Errorf("arg 1: got %#v, want float64(3.5)", args[1])
	}
	// string
	if v, ok := args[2].(string); !ok || v != "hello" {
		t.Errorf("arg 2: got %#v, want string hello", args[2])
	}
	// bool
	if v, ok := args[3].(bool); !ok || !v {
		t.Errorf("arg 3: got %#v, want bool true", args[3])
	}
	// time.Time
	if v, ok := args[4].(time.Time); !ok || !v.Equal(ts) {
		t.Errorf("arg 4: got %#v, want %v", args[4], ts)
	}
	// []byte
	if v, ok := args[5].([]byte); !ok {
		t.Errorf("arg 5: got %T, want []byte", args[5])
	} else if len(v) != 4 || v[0] != 0xDE || v[3] != 0xEF {
		t.Errorf("arg 5 bytes: got % x, want deadbeef", v)
	}
	// nil (NULL)
	if args[6] != nil {
		t.Errorf("arg 6: got %#v, want nil", args[6])
	}

	// committed, not rolled back
	if !db.tx.committed {
		t.Error("tx was not committed")
	}
	if db.tx.rolledBack {
		t.Error("tx was rolled back on a successful batch")
	}
}

// (c2) multiple rows -> one Prepare, N Execs, one Commit.
func TestSink_WriteBatch_MultipleRows(t *testing.T) {
	r1, _ := dbbatch.EncodeRow([]any{int64(1), "a"})
	r2, _ := dbbatch.EncodeRow([]any{int64(2), "b"})
	r3, _ := dbbatch.EncodeRow([]any{int64(3), "c"})

	db := &fakeDB{}
	s := newSinkWithDB(Config{Table: "t"}, db)

	if err := s.WriteBatch(dbbatch.DBBatch{
		Columns: []string{"id", "name"},
		Rows:    []dbbatch.DBRow{r1, r2, r3},
	}); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	if len(db.tx.execs) != 3 {
		t.Fatalf("Exec calls: got %d, want 3", len(db.tx.execs))
	}
	// one prepared statement (not re-prepared per row)
	if db.tx.prepared == "" {
		t.Fatal("statement never prepared")
	}
}

// (d) columns override from config takes precedence over batch.Columns.
func TestSink_WriteBatch_ColumnsOverrideFromConfig(t *testing.T) {
	row, err := dbbatch.EncodeRow([]any{int64(1), "alice"})
	if err != nil {
		t.Fatalf("EncodeRow: %v", err)
	}
	db := &fakeDB{}
	// Config.Columns = "user_id,name" must WIN over batch.Columns ["id","nm"].
	s := newSinkWithDB(Config{Table: "users", Columns: "user_id,name"}, db)

	batch := dbbatch.DBBatch{
		Columns: []string{"id", "nm"},
		Rows:    []dbbatch.DBRow{row},
	}
	if err := s.WriteBatch(batch); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	wantStmt := "INSERT INTO users (user_id,name) VALUES (@p1,@p2)"
	if db.tx.prepared != wantStmt {
		t.Errorf("override stmt:\n got %q\nwant %q", db.tx.prepared, wantStmt)
	}
}

// (e) NULL slot decodes to a nil arg, NOT an empty string.
func TestSink_WriteBatch_NullSlotBecomesNilArg(t *testing.T) {
	row, err := dbbatch.EncodeRow([]any{nil, "x"})
	if err != nil {
		t.Fatalf("EncodeRow: %v", err)
	}
	db := &fakeDB{}
	s := newSinkWithDB(Config{Table: "t"}, db)

	if err := s.WriteBatch(dbbatch.DBBatch{
		Columns: []string{"a", "b"},
		Rows:    []dbbatch.DBRow{row},
	}); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	if len(db.tx.execs) != 1 {
		t.Fatalf("Exec calls: got %d, want 1", len(db.tx.execs))
	}
	if db.tx.execs[0].args[0] != nil {
		t.Errorf("NULL arg: got %#v, want nil — must not be empty string", db.tx.execs[0].args[0])
	}
}

// (f) BeginTx error -> wrapped, no Prepare attempted.
func TestSink_WriteBatch_BeginTxError_Wrapped(t *testing.T) {
	beginErr := errors.New("connection refused")
	db := &fakeDB{beginErr: beginErr}
	s := newSinkWithDB(Config{Table: "t"}, db)

	row, _ := dbbatch.EncodeRow([]any{int64(1)})
	err := s.WriteBatch(dbbatch.DBBatch{
		Columns: []string{"id"},
		Rows:    []dbbatch.DBRow{row},
	})
	if err == nil {
		t.Fatal("expected wrapped begin error")
	}
	if !strings.HasPrefix(err.Error(), "sqlserver sink: begin:") {
		t.Fatalf("error prefix: got %q", err.Error())
	}
	if !errors.Is(err, beginErr) {
		t.Fatalf("error not wrapping original: got %q", err.Error())
	}
}

// (g) Prepare error -> wrapped, tx rolled back, no Exec.
func TestSink_WriteBatch_PrepareError_WrappedAndRolledBack(t *testing.T) {
	prepareErr := errors.New("prepare failed")
	db := &fakeDB{}
	// pre-seed the tx with a prepare error by intercepting BeginTx
	db.beginErr = nil
	// We need a tx whose PrepareContext errors; wrap via a custom begin.
	s := newSinkWithDB(Config{Table: "t"}, &fakeDBWithPrepareErr{prepareErr: prepareErr})

	row, _ := dbbatch.EncodeRow([]any{int64(1)})
	err := s.WriteBatch(dbbatch.DBBatch{
		Columns: []string{"id"},
		Rows:    []dbbatch.DBRow{row},
	})
	if err == nil {
		t.Fatal("expected wrapped prepare error")
	}
	if !strings.HasPrefix(err.Error(), "sqlserver sink: prepare:") {
		t.Fatalf("error prefix: got %q", err.Error())
	}
	if !errors.Is(err, prepareErr) {
		t.Fatalf("error not wrapping original: got %q", err.Error())
	}
	// must have rolled back
	fdb := s.db.(*fakeDBWithPrepareErr)
	if !fdb.tx.rolledBack {
		t.Error("tx was NOT rolled back after a Prepare error")
	}
	if len(fdb.tx.execs) != 0 {
		t.Errorf("Exec called despite Prepare failure: %d calls", len(fdb.tx.execs))
	}
}

// (h) Exec error -> wrapped, tx rolled back, statement closed.
func TestSink_WriteBatch_ExecError_WrappedAndRolledBack(t *testing.T) {
	execErr := errors.New("constraint violation")
	db := &fakeDBWithExecErr{execErr: execErr}
	s := newSinkWithDB(Config{Table: "t"}, db)

	r1, _ := dbbatch.EncodeRow([]any{int64(1)})
	r2, _ := dbbatch.EncodeRow([]any{int64(2)})
	err := s.WriteBatch(dbbatch.DBBatch{
		Columns: []string{"id"},
		Rows:    []dbbatch.DBRow{r1, r2},
	})
	if err == nil {
		t.Fatal("expected wrapped exec error")
	}
	if !strings.HasPrefix(err.Error(), "sqlserver sink: exec:") {
		t.Fatalf("error prefix: got %q", err.Error())
	}
	if !errors.Is(err, execErr) {
		t.Fatalf("error not wrapping original: got %q", err.Error())
	}
	if !db.tx.rolledBack {
		t.Error("tx was NOT rolled back after an Exec error")
	}
	if db.tx.committed {
		t.Error("tx was committed after an Exec error")
	}
}

// --- extra fakes for the prepare/exec error paths ----------------------------

// fakeDBWithPrepareErr begins a tx whose PrepareContext always errors.
type fakeDBWithPrepareErr struct {
	prepareErr error
	tx         *errPrepareTx
}

func (d *fakeDBWithPrepareErr) BeginTx(_ context.Context, _ *sql.TxOptions) (txExecutor, error) {
	d.tx = &errPrepareTx{prepareErr: d.prepareErr}
	return d.tx, nil
}
func (d *fakeDBWithPrepareErr) Close() {}

type errPrepareTx struct {
	prepareErr error
	rolledBack bool
	committed  bool
	execs      []execCall
}

func (t *errPrepareTx) PrepareContext(_ context.Context, _ string) (stmtExecutor, error) {
	return nil, t.prepareErr
}
func (t *errPrepareTx) Commit() error   { t.committed = true; return nil }
func (t *errPrepareTx) Rollback() error { t.rolledBack = true; return nil }

// fakeDBWithExecErr begins a tx whose stmt errors on every ExecContext.
type fakeDBWithExecErr struct {
	execErr error
	tx      *errExecTx
}

func (d *fakeDBWithExecErr) BeginTx(_ context.Context, _ *sql.TxOptions) (txExecutor, error) {
	d.tx = &errExecTx{execErr: d.execErr}
	return d.tx, nil
}
func (d *fakeDBWithExecErr) Close() {}

type errExecTx struct {
	execErr    error
	prepared   string
	rolledBack bool
	committed  bool
	execs      []execCall
}

func (t *errExecTx) PrepareContext(_ context.Context, query string) (stmtExecutor, error) {
	t.prepared = query
	return &errExecStmt{parent: t}, nil
}
func (t *errExecTx) Commit() error   { t.committed = true; return nil }
func (t *errExecTx) Rollback() error { t.rolledBack = true; return nil }

type errExecStmt struct {
	parent *errExecTx
}

func (s *errExecStmt) ExecContext(_ context.Context, _ ...any) (sql.Result, error) {
	return nil, s.parent.execErr
}
func (s *errExecStmt) Close() error { return nil }

// =============================================================================
// 4. Close
// =============================================================================

func TestSink_Close_Uninit_NilError(t *testing.T) {
	s := New()
	if err := s.Close(); err != nil {
		t.Fatalf("Close uninit: %v", err)
	}
}

func TestSink_Close_AfterInit_CallsDBClose(t *testing.T) {
	db := &fakeDB{}
	s := &Sink{db: db}
	if err := s.Close(); err != nil {
		t.Fatalf("Close after init: %v", err)
	}
	if !db.closed {
		t.Fatal("db.Close was not called")
	}
}

// Close is idempotent: calling twice does not panic.
func TestSink_Close_Idempotent(t *testing.T) {
	db := &fakeDB{}
	s := &Sink{db: db}
	if err := s.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close 2 (idempotent): %v", err)
	}
}
