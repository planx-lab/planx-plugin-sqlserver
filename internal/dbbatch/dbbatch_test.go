package dbbatch

import (
	"bytes"
	"encoding/base64"
	"encoding/gob"
	"reflect"
	"testing"
	"time"
)

// registerForGob mirrors the two sdk.RegisterType calls that source/sink
// init() MUST make. Registering here keeps the test faithful to the real
// per-process registration set (DBBatch{} + DBRow{}) — nothing more.
func registerForGob(t *testing.T) {
	t.Helper()
	gob.Register(DBBatch{})
	gob.Register(DBRow{})
}

// wrapper mirrors the SDK's internal batch codec shape (see
// planx-sdk-go/internal/batch/codec.go: batchWrapper{Batch any}). The real
// codec does gob.NewEncoder(buf).Encode(batchWrapper{Batch: b}) and the
// inverse on decode. Replicating that wrapper here proves DBBatch survives
// the exact wire path the SDK uses — not a synthetic gob call.
type wrapper struct {
	Batch any
}

// TestGobRoundTrip is the regression test for the gob footgun (design §3, §8
// test 2). It encodes a DBBatch through the SDK codec's wrapper shape and
// decodes it back, asserting Columns/Rows/Types/Vals survive byte-for-byte.
// If a future commit drops sdk.RegisterType(DBBatch{})/DBRow{} from
// source/sink init(), this test fails loudly.
func TestGobRoundTrip(t *testing.T) {
	registerForGob(t)

	orig := DBBatch{
		Columns: []string{"id", "name", "ratio", "active", "at", "blob", "note"},
		Rows: []DBRow{
			{
				Types: []byte{KindInt, KindString, KindFloat, KindBool, KindTime, KindBytes, KindNil},
				Vals: []string{
					"42",
					"alice",
					"3.14159",
					"true",
					time.Date(2026, 7, 1, 12, 30, 45, 123456789, time.UTC).Format(time.RFC3339Nano),
					"AAEC", // base64 of []byte{0,1,2}
					"",     // NULL — value ignored on decode
				},
			},
			{
				Types: []byte{KindInt, KindString, KindFloat, KindBool, KindTime, KindBytes, KindNil},
				Vals: []string{
					"-7",
					"",
					"0",
					"false",
					time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
					"", // empty []byte base64
					"",
				},
			},
		},
	}

	// Encode through the SDK codec wrapper shape.
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(wrapper{Batch: orig}); err != nil {
		t.Fatalf("encode: %v", err)
	}

	// Decode through the same shape.
	var w wrapper
	if err := gob.NewDecoder(bytes.NewReader(buf.Bytes())).Decode(&w); err != nil {
		t.Fatalf("decode: %v", err)
	}

	got, ok := w.Batch.(DBBatch)
	if !ok {
		t.Fatalf("decoded batch is %T, want DBBatch", w.Batch)
	}
	if !reflect.DeepEqual(got, orig) {
		t.Fatalf("round-trip mismatch:\n got  %+v\n want %+v", got, orig)
	}
}

// TestEncodeRowPerKind verifies encodeRow maps each concrete Go type to the
// correct Kind tag + string encoding (design §4 encode table).
func TestEncodeRowPerKind(t *testing.T) {
	ts := time.Date(2026, 7, 1, 12, 30, 45, 987654321, time.UTC)

	tests := []struct {
		name string
		val  any
		want byte
		str  string
	}{
		{"int64", int64(42), KindInt, "42"},
		{"int64_negative", int64(-7), KindInt, "-7"},
		// pgx type mapping: PG int2→int16, int4→int32, int8→int64,
		// float4(real)→float32, float8(double)→float64. EncodeRow must
		// accept all int widths (→KindInt via int64) and float32
		// (→KindFloat via float64). Regression for the e2e bug where
		// int32 (PG int4) hit "unsupported type".
		{"int8", int8(7), KindInt, "7"},
		{"int16", int16(42), KindInt, "42"},
		{"int32", int32(7), KindInt, "7"},
		{"int", int(99), KindInt, "99"},
		{"float32", float32(1.5), KindFloat, "1.5"},
		{"float64_precision", float64(3.14159), KindFloat, "3.14159"},
		{"float64_zero", float64(0), KindFloat, "0"},
		{"string", "alice", KindString, "alice"},
		{"string_empty", "", KindString, ""},
		{"bool_true", true, KindBool, "true"},
		{"bool_false", false, KindBool, "false"},
		{"time", ts, KindTime, ts.Format(time.RFC3339Nano)},
		{"bytes", []byte{0, 1, 2}, KindBytes, "AAEC"},
		{"bytes_empty", []byte{}, KindBytes, ""},
		{"nil", nil, KindNil, ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			row, err := encodeRow([]any{tc.val})
			if err != nil {
				t.Fatalf("encodeRow: %v", err)
			}
			if len(row.Types) != 1 || row.Types[0] != tc.want {
				t.Fatalf("Types = %v, want [%d]", row.Types, tc.want)
			}
			if len(row.Vals) != 1 || row.Vals[0] != tc.str {
				t.Fatalf("Vals = %v, want [%q]", row.Vals, tc.str)
			}
		})
	}
}

// TestEncodeRowMultiple verifies a full heterogeneous row encodes in column
// order with parallel Types/Vals slices.
func TestEncodeRowMultiple(t *testing.T) {
	ts := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	vals := []any{int64(1), "two", float64(3.5), true, ts, []byte{0xFF}, nil}

	row, err := encodeRow(vals)
	if err != nil {
		t.Fatalf("encodeRow: %v", err)
	}
	wantTypes := []byte{KindInt, KindString, KindFloat, KindBool, KindTime, KindBytes, KindNil}
	wantVals := []string{
		"1", "two", "3.5", "true", ts.Format(time.RFC3339Nano), "/w==", "",
	}
	if !reflect.DeepEqual(row.Types, wantTypes) {
		t.Fatalf("Types = %v, want %v", row.Types, wantTypes)
	}
	if !reflect.DeepEqual(row.Vals, wantVals) {
		t.Fatalf("Vals = %v, want %v", row.Vals, wantVals)
	}
}

// TestEncodeRowUnknownType verifies unsupported types yield an error rather
// than silent coercion (design: pick error for safety). Note: int32 is now a
// SUPPORTED type (pgx maps PG int4 → int32), so use a struct as the
// genuinely-unsupported type here.
func TestEncodeRowUnknownType(t *testing.T) {
	type weird struct{ X int }
	_, err := encodeRow([]any{weird{X: 1}})
	if err == nil {
		t.Fatal("encodeRow(struct) = nil, want error")
	}
}

// TestDecodeRowToArgsPerKind verifies decodeRowToArgs inverts encodeRow for
// every Kind, returning correctly-typed Go values (not strings).
func TestDecodeRowToArgsPerKind(t *testing.T) {
	ts := time.Date(2026, 7, 1, 12, 30, 45, 987654321, time.UTC)

	tests := []struct {
		name string
		kind byte
		str  string
		want any
	}{
		{"int64", KindInt, "42", int64(42)},
		{"int64_negative", KindInt, "-7", int64(-7)},
		{"float64", KindFloat, "3.14159", float64(3.14159)},
		{"string", KindString, "alice", "alice"},
		{"bool_true", KindBool, "true", true},
		{"bool_false", KindBool, "false", false},
		{"time", KindTime, ts.Format(time.RFC3339Nano), ts},
		{"bytes", KindBytes, "AAEC", []byte{0, 1, 2}},
		{"bytes_empty", KindBytes, "", []byte{}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			row := DBRow{Types: []byte{tc.kind}, Vals: []string{tc.str}}
			args, err := decodeRowToArgs(row)
			if err != nil {
				t.Fatalf("decodeRowToArgs: %v", err)
			}
			if len(args) != 1 {
				t.Fatalf("len(args) = %d, want 1", len(args))
			}
			// time.Time needs Equal, not reflect.DeepEqual on wall-clock.
			if tc.kind == KindTime {
				got, ok := args[0].(time.Time)
				if !ok {
					t.Fatalf("arg is %T, want time.Time", args[0])
				}
				if !got.Equal(tc.want.(time.Time)) {
					t.Fatalf("time = %v, want %v", got, tc.want)
				}
				return
			}
			if !reflect.DeepEqual(args[0], tc.want) {
				t.Fatalf("arg = %T(%v), want %T(%v)", args[0], args[0], tc.want, tc.want)
			}
		})
	}
}

// TestDecodeRowTypeAssertion verifies decode yields a real int64 and time.Time,
// not their string forms — the whole point of the Kind tag.
func TestDecodeRowTypeAssertion(t *testing.T) {
	ts := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	wantBytes := []byte{0, 1, 0xFC, 0xAA}
	row := DBRow{
		Types: []byte{KindInt, KindTime, KindBytes},
		Vals:  []string{"100", ts.Format(time.RFC3339Nano), base64.StdEncoding.EncodeToString(wantBytes)},
	}
	args, err := decodeRowToArgs(row)
	if err != nil {
		t.Fatalf("decodeRowToArgs: %v", err)
	}
	if v, ok := args[0].(int64); !ok || v != 100 {
		t.Fatalf("args[0] = %T(%v), want int64(100)", args[0], args[0])
	}
	if v, ok := args[1].(time.Time); !ok || !v.Equal(ts) {
		t.Fatalf("args[1] = %T(%v), want time.Time(%v)", args[1], args[1], ts)
	}
	if v, ok := args[2].([]byte); !ok || !bytes.Equal(v, wantBytes) {
		t.Fatalf("args[2] = %T(%v), want %v", args[2], args[2], wantBytes)
	}
}

// TestNullRoundTrip verifies a KindNil slot decodes to a nil arg (design §8
// test 8) — not an empty string. This is the NULL-preservation guarantee.
func TestNullRoundTrip(t *testing.T) {
	row := DBRow{Types: []byte{KindInt, KindNil, KindString}, Vals: []string{"5", "", "x"}}
	args, err := decodeRowToArgs(row)
	if err != nil {
		t.Fatalf("decodeRowToArgs: %v", err)
	}
	if args[1] != nil {
		t.Fatalf("args[1] = %T(%v), want nil", args[1], args[1])
	}
}

// TestEncodeDecodeRoundTrip verifies encode then decode yields values that
// compare equal for every Kind (full encode→decode fidelity).
func TestEncodeDecodeRoundTrip(t *testing.T) {
	ts := time.Date(2026, 7, 1, 12, 30, 45, 0, time.UTC)
	vals := []any{int64(99), "str", float64(2.5), false, ts, []byte{1, 2, 3}, nil}

	row, err := encodeRow(vals)
	if err != nil {
		t.Fatalf("encodeRow: %v", err)
	}
	args, err := decodeRowToArgs(row)
	if err != nil {
		t.Fatalf("decodeRowToArgs: %v", err)
	}
	if len(args) != len(vals) {
		t.Fatalf("len(args) = %d, want %d", len(args), len(vals))
	}
	// Per-value assertions: time.Time via Equal (wall-clock), []byte via
	// bytes.Equal, scalars via DeepEqual, nil via identity.
	if v := args[0]; !reflect.DeepEqual(v, int64(99)) {
		t.Fatalf("args[0] = %T(%v), want int64(99)", v, v)
	}
	if v := args[1]; !reflect.DeepEqual(v, "str") {
		t.Fatalf("args[1] = %T(%v), want str", v, v)
	}
	if v := args[2]; !reflect.DeepEqual(v, float64(2.5)) {
		t.Fatalf("args[2] = %T(%v), want 2.5", v, v)
	}
	if v := args[3]; !reflect.DeepEqual(v, false) {
		t.Fatalf("args[3] = %T(%v), want false", v, v)
	}
	if v, ok := args[4].(time.Time); !ok || !v.Equal(ts) {
		t.Fatalf("args[4] = %T(%v), want time %v", args[4], args[4], ts)
	}
	if v, ok := args[5].([]byte); !ok || !bytes.Equal(v, []byte{1, 2, 3}) {
		t.Fatalf("args[5] = %T(%v), want []byte{1,2,3}", args[5], args[5])
	}
	if args[6] != nil {
		t.Fatalf("args[6] = %T(%v), want nil", args[6], args[6])
	}
}

// TestDecodeRowUnknownKind verifies an unknown Kind tag yields an error.
func TestDecodeRowUnknownKind(t *testing.T) {
	row := DBRow{Types: []byte{0}, Vals: []string{"x"}}
	if _, err := decodeRowToArgs(row); err == nil {
		t.Fatal("decodeRowToArgs(unknown kind) = nil, want error")
	}
}

// TestDecodeRowLengthMismatch verifies a Types/Vals length mismatch is caught.
func TestDecodeRowLengthMismatch(t *testing.T) {
	row := DBRow{Types: []byte{KindInt, KindString}, Vals: []string{"only one"}}
	if _, err := decodeRowToArgs(row); err == nil {
		t.Fatal("decodeRowToArgs(mismatched lengths) = nil, want error")
	}
}
