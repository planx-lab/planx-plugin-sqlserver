// Package dbbatch defines the gob-registered batch payload for DB row data.
//
// This file is copied VERBATIM into planx-plugin-sqlserver (design §3 mandates
// byte-identical: gob matches by type name AND wire structure, so the two repos
// must keep field names, types, and order identical for cross-connector
// pipelines like postgres-source → sqlserver-sink to work). It is therefore
// strictly driver-agnostic — no pgx/mssqldb imports, ever.
package dbbatch

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"time"
)

// DBBatch is the gob-registered batch payload for DB row data. Carries the
// column schema once at the top plus per-row type-tagged string slots.
type DBBatch struct {
	Columns []string // ordered column names (schema for every row in this batch)
	Rows    []DBRow
}

// DBRow is one row. Types and Vals are parallel to DBBatch.Columns.
type DBRow struct {
	Types []byte   // per-column kind tag (see Kind* constants)
	Vals  []string // per-column string-encoded value
}

// Kind tags. Stable across repos. Single byte, parallel to Columns.
const (
	KindInt    byte = 1 // int64     -> Vals: strconv.FormatInt(v, 10)
	KindFloat  byte = 2 // float64   -> Vals: strconv.FormatFloat(v, 'f', -1, 64)
	KindString byte = 3 // string    -> Vals: as-is
	KindBool   byte = 4 // bool      -> Vals: strconv.FormatBool(v)
	KindTime   byte = 5 // time.Time -> Vals: t.Format(time.RFC3339Nano)
	KindBytes  byte = 6 // []byte    -> Vals: base64.StdEncoding.EncodeToString(v)
	KindNil    byte = 7 // SQL NULL  -> Vals: "" (value ignored on decode)
)

// EncodeRow converts a slice of concrete Go values (as returned by a DB
// driver's scan) into a DBRow with per-column Kind tags and string-encoded
// slots. Unsupported concrete types return an error — do NOT coerce silently,
// because a missed type means a wrong Kind on the wire (design §4 encode table).
// Exported so the source (scan time) can call it; the sink calls decodeRowToArgs.
func EncodeRow(values []any) (DBRow, error) {
	types := make([]byte, len(values))
	vals := make([]string, len(values))
	for i, v := range values {
		switch x := v.(type) {
		// pgx decodes PG int2→int16, int4→int32, int8→int64; mssqldb has
		// its own width mapping. Coerce ALL integer widths to int64 so
		// they land on KindInt (the canonical widened form on the wire).
		case int64:
			types[i] = KindInt
			vals[i] = strconv.FormatInt(x, 10)
		case int:
			types[i] = KindInt
			vals[i] = strconv.FormatInt(int64(x), 10)
		case int32:
			types[i] = KindInt
			vals[i] = strconv.FormatInt(int64(x), 10)
		case int16:
			types[i] = KindInt
			vals[i] = strconv.FormatInt(int64(x), 10)
		case int8:
			types[i] = KindInt
			vals[i] = strconv.FormatInt(int64(x), 10)
		// float4(real)→float32 on pgx; widen to float64 for KindFloat.
		case float64:
			types[i] = KindFloat
			vals[i] = strconv.FormatFloat(x, 'f', -1, 64)
		case float32:
			types[i] = KindFloat
			vals[i] = strconv.FormatFloat(float64(x), 'f', -1, 64)
		case string:
			types[i] = KindString
			vals[i] = x
		case bool:
			types[i] = KindBool
			vals[i] = strconv.FormatBool(x)
		case time.Time:
			types[i] = KindTime
			vals[i] = x.Format(time.RFC3339Nano)
		case []byte:
			types[i] = KindBytes
			vals[i] = base64.StdEncoding.EncodeToString(x)
		case nil:
			types[i] = KindNil
			vals[i] = ""
		default:
			return DBRow{}, fmt.Errorf("dbbatch: unsupported type %T", v)
		}
	}
	return DBRow{Types: types, Vals: vals}, nil
}

// encodeRow is the unexported alias kept for internal/test callers. It
// delegates to the now-exported EncodeRow.
func encodeRow(values []any) (DBRow, error) { return EncodeRow(values) }

// DecodeRowToArgs inverts EncodeRow: reads each Kind tag and converts the
// string slot back to the typed Go value the driver wants as an INSERT param
// (int64, time.Time, []byte, nil, ...). This is where type fidelity pays off
// — the INSERT params are correctly typed rather than guessed from a string.
// Exported so the sink (WriteBatch time) can call it.
func DecodeRowToArgs(row DBRow) ([]any, error) {
	return decodeRowToArgs(row)
}

// decodeRowToArgs is the unexported implementation; DecodeRowToArgs is the
// exported alias. Kept so internal/test callers in the source package can keep
// using the lowercase name unchanged.
func decodeRowToArgs(row DBRow) ([]any, error) {
	if len(row.Types) != len(row.Vals) {
		return nil, fmt.Errorf("dbbatch: row length mismatch: %d types, %d vals", len(row.Types), len(row.Vals))
	}
	args := make([]any, len(row.Types))
	for i, k := range row.Types {
		s := row.Vals[i]
		switch k {
		case KindInt:
			v, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("dbbatch: decode int: %w", err)
			}
			args[i] = v
		case KindFloat:
			v, err := strconv.ParseFloat(s, 64)
			if err != nil {
				return nil, fmt.Errorf("dbbatch: decode float: %w", err)
			}
			args[i] = v
		case KindString:
			args[i] = s
		case KindBool:
			v, err := strconv.ParseBool(s)
			if err != nil {
				return nil, fmt.Errorf("dbbatch: decode bool: %w", err)
			}
			args[i] = v
		case KindTime:
			v, err := time.Parse(time.RFC3339Nano, s)
			if err != nil {
				return nil, fmt.Errorf("dbbatch: decode time: %w", err)
			}
			args[i] = v
		case KindBytes:
			v, err := base64.StdEncoding.DecodeString(s)
			if err != nil {
				return nil, fmt.Errorf("dbbatch: decode bytes: %w", err)
			}
			args[i] = v
		case KindNil:
			args[i] = nil
		default:
			return nil, fmt.Errorf("dbbatch: unknown kind tag %d", k)
		}
	}
	return args, nil
}
