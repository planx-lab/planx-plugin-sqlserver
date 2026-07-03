package source

import (
	"context"
	"database/sql"
	"fmt"

	// go-mssqldb registers the "sqlserver" database/sql driver on import via its
	// init(); the blank import is required so sql.Open(driverName, ...) resolves.
	// The "mssql" alias is deprecated and has unfixed bugs (design §2).
	_ "github.com/microsoft/go-mssqldb"
)

// querier is the DB-free testability seam (design §8). The production Source
// holds a *sql.DB-backed querier; tests inject a fakeQuerier that yields canned
// rows. This is THE single decision that lets unit tests run without SQL Server.
type querier interface {
	Query(ctx context.Context, query string, args ...any) (rowsIterator, error)
	Close()
}

// rowsIterator mirrors the subset of *sql.Rows the Source needs. The seam is
// ScanValues (NOT pgx's Values): mssql scans into *interface{} (new(any)) ptrs
// -> rows.Scan -> deref. The production mssqlRows returns the already-deref'd
// []any from this method; the fake short-circuits and yields the same deref'd
// []any.
type rowsIterator interface {
	Next() bool
	Columns() []string
	ScanValues() ([]any, error)
	Err() error
	Close()
}

// mssqlQuerier is the production querier, backed by a *sql.DB.
type mssqlQuerier struct {
	db *sql.DB
}

func (q *mssqlQuerier) Query(ctx context.Context, query string, args ...any) (rowsIterator, error) {
	rows, err := q.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &mssqlRows{rows: rows}, nil
}

func (q *mssqlQuerier) Close() { q.db.Close() }

// mssqlRows adapts *sql.Rows to rowsIterator. ScanValues scans each column
// into a *interface{} (new(any)): NULL → nil, non-NULL → the driver's concrete
// Go type (int64, float64, string, bool, time.Time, []byte). This is the
// database/sql-native equivalent of pgx's Values() and yields the same concrete
// types for dbbatch.EncodeRow. Scanning into new(any) is NULL-safe — unlike
// reflect.New(ct.ScanType()), which creates non-nullable types (e.g. string for
// NVARCHAR) that database/sql refuses to fill on NULL.
type mssqlRows struct {
	rows     *sql.Rows
	colTypes []*sql.ColumnType
}

func (r *mssqlRows) Next() bool { return r.rows.Next() }

func (r *mssqlRows) Columns() []string {
	cols, _ := r.rows.Columns()
	return cols
}

// ScanValues scans each column into a *interface{}. NULL columns become nil
// (which dbbatch.EncodeRow maps to KindNil); non-NULL columns arrive as the
// driver's concrete type (int64, float64, string, bool, time.Time, []byte).
// Column types are lazily captured on first call to size the slice.
func (r *mssqlRows) ScanValues() ([]any, error) {
	if r.colTypes == nil {
		r.colTypes, _ = r.rows.ColumnTypes()
	}
	ptrs := make([]any, len(r.colTypes))
	for i := range ptrs {
		ptrs[i] = new(any) // *interface{} — NULL-safe, heterogeneous
	}
	if err := r.rows.Scan(ptrs...); err != nil {
		return nil, fmt.Errorf("sqlserver source: scan: %w", err)
	}
	vals := make([]any, len(ptrs))
	for i, p := range ptrs {
		vals[i] = *(p.(*any))
	}
	return vals, nil
}

func (r *mssqlRows) Err() error { return r.rows.Err() }
func (r *mssqlRows) Close()     { r.rows.Close() }

// driverName is the database/sql registration name for go-mssqldb. The "mssql"
// alias is deprecated and has unfixed bugs (design §2).
const driverName = "sqlserver"
