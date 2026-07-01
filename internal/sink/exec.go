package sink

import (
	"context"
	"database/sql"
)

// dbExecutor / txExecutor / stmtExecutor form the DB-free testability seam
// (design §8). The production Sink holds a dbExecutor (real = *sql.DB wrapped in
// mssqlDB); tests inject a fakeDB/fakeTx/fakeStmt that record the INSERT
// statement and per-row typed args. This is THE single decision that lets unit
// tests run without SQL Server: no go-mssqldb import, no sql.Open in _test.go.
//
// The seam mirrors the *sql.DB subset the prepared-INSERT-in-tx path needs
// (design §5): BeginTx -> PrepareContext -> loop ExecContext -> Commit, with
// Rollback on any error.

type dbExecutor interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (txExecutor, error)
	Close()
}

type txExecutor interface {
	PrepareContext(ctx context.Context, query string) (stmtExecutor, error)
	Commit() error
	Rollback() error
}

type stmtExecutor interface {
	ExecContext(ctx context.Context, args ...any) (sql.Result, error)
	Close() error
}

// mssqlDB is the production dbExecutor, backed by a *sql.DB. The wrapper exists
// only so *sql.DB can satisfy the unexported dbExecutor interface (an unexported
// wrapper keeps *sql.DB out of the Sink's field type and out of _test.go).
type mssqlDB struct {
	db *sql.DB
}

func (d *mssqlDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (txExecutor, error) {
	tx, err := d.db.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &mssqlTx{tx: tx}, nil
}

func (d *mssqlDB) Close() { d.db.Close() }

// mssqlTx is the production txExecutor, backed by a *sql.Tx.
type mssqlTx struct {
	tx *sql.Tx
}

func (t *mssqlTx) PrepareContext(ctx context.Context, query string) (stmtExecutor, error) {
	stmt, err := t.tx.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return &mssqlStmt{stmt: stmt}, nil
}

func (t *mssqlTx) Commit() error   { return t.tx.Commit() }
func (t *mssqlTx) Rollback() error { return t.tx.Rollback() }

// mssqlStmt is the production stmtExecutor, backed by a *sql.Stmt.
type mssqlStmt struct {
	stmt *sql.Stmt
}

func (s *mssqlStmt) ExecContext(ctx context.Context, args ...any) (sql.Result, error) {
	return s.stmt.ExecContext(ctx, args...)
}

func (s *mssqlStmt) Close() error { return s.stmt.Close() }

// driverName is the database/sql registration name for go-mssqldb. The "mssql"
// alias is deprecated and has unfixed bugs (design §2).
const driverName = "sqlserver"
