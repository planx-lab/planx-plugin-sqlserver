package source

import (
	"context"
	"database/sql"
	"reflect"

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
// ScanValues (NOT pgx's Values): mssql scans via
// reflect.New(ct.ScanType()).Interface() ptrs -> rows.Scan -> deref. The
// production mssqlRows returns the already-deref'd []any from this method; the
// fake short-circuits the reflect path and yields the same deref'd []any.
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

// mssqlRows adapts *sql.Rows to rowsIterator. ScanValues allocates a pointer
// per column via reflect on the column's ScanType, scans into them, then
// dereferences each into the returned []any. This is the database/sql-native
// equivalent of pgx's Values() and yields concrete Go types (int64, float64,
// string, bool, time.Time, []byte, nil) for dbbatch.EncodeRow.
type mssqlRows struct {
	rows     *sql.Rows
	colTypes []*sql.ColumnType
}

func (r *mssqlRows) Next() bool { return r.rows.Next() }

func (r *mssqlRows) Columns() []string {
	cols, _ := r.rows.Columns()
	return cols
}

// ScanValues performs the reflect-Scan then deref (design §4 scanRow, mssql path).
// Column types are lazily captured on first call.
func (r *mssqlRows) ScanValues() ([]any, error) {
	if r.colTypes == nil {
		r.colTypes, _ = r.rows.ColumnTypes()
	}
	ptrs := make([]any, len(r.colTypes))
	for i, ct := range r.colTypes {
		ptrs[i] = reflect.New(ct.ScanType()).Interface()
	}
	if err := r.rows.Scan(ptrs...); err != nil {
		return nil, err
	}
	vals := make([]any, len(ptrs))
	for i, p := range ptrs {
		vals[i] = derefValue(p)
	}
	return vals, nil
}

func (r *mssqlRows) Err() error { return r.rows.Err() }
func (r *mssqlRows) Close()     { r.rows.Close() }

// derefValue unwraps a reflect.New pointer to its concrete value. A typed
// pointer to the zero value remains the zero value (e.g. *int64 -> int64(0)),
// which dbbatch.EncodeRow handles by kind. NULL columns arrive as a typed
// pointer whose driver Value is nil; mssqldb surfaces those as nil here after
// deref when the ScanType is an interface or the driver returns nil.
func derefValue(p any) any {
	v := reflect.ValueOf(p)
	for v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface {
		if v.IsNil() {
			return nil
		}
		v = v.Elem()
	}
	return v.Interface()
}

// driverName is the database/sql registration name for go-mssqldb. The "mssql"
// alias is deprecated and has unfixed bugs (design §2).
const driverName = "sqlserver"
