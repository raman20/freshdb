package lens

import (
	"encoding/json"
	"math"
	"math/big"
	"testing"
)

func TestCanonicalJSONNumbers(t *testing.T) {
	same := [][]any{
		{json.Number("1"), json.Number("1.0"), json.Number("10e-1"), json.Number("0.1E1"), 1, 1.0, int64(1), uint8(1)},
		{json.Number("0"), json.Number("-0"), json.Number("0.000"), json.Number("0e99999999999999999999"), 0.0, math.Copysign(0, -1)},
		{json.Number("100"), json.Number("1e2"), json.Number("1E+2"), 100.0},
		{json.Number("0.1"), 0.1},
		{json.Number("-2.50"), -2.5},
		{map[string]any{"b": json.Number("2.0"), "a": []any{json.Number("1e0")}}, map[string]any{"a": []any{1}, "b": 2}},
	}
	for _, g := range same {
		c0, ok := CanonicalJSON(g[0])
		if !ok {
			t.Fatalf("%v: not canonical", g[0])
		}
		for _, v := range g[1:] {
			if c, _ := CanonicalJSON(v); c != c0 {
				t.Errorf("%#v -> %s, %#v -> %s: want equal", g[0], c0, v, c)
			}
		}
	}
	differ := [][2]any{
		{json.Number("18446744073709551616"), json.Number("18446744073709551617")},
		{json.Number("0.1"), json.Number("0.10000000000000001")},
		{json.Number("1"), "1"},
		{json.Number("1"), true},
		{json.Number("1e9223372036854775807"), json.Number("1e9223372036854775806")},
	}
	for _, d := range differ {
		a, _ := CanonicalJSON(d[0])
		b, _ := CanonicalJSON(d[1])
		if a == b {
			t.Errorf("%#v and %#v both -> %s", d[0], d[1], a)
		}
	}
}

// canonNumber agrees with exact rational equality on valid JSON numbers.
func FuzzCanonNumber(f *testing.F) {
	for _, s := range []string{"1", "1.0", "-0", "1e2", "100", "0.001e3", "12345678901234567890", "-1.5E-3"} {
		f.Add(s, "1")
	}
	f.Fuzz(func(t *testing.T, a, b string) {
		if len(a) > 40 || len(b) > 40 || !isNumber(a) || !isNumber(b) {
			return
		}
		ra, oka := new(big.Rat).SetString(a)
		rb, okb := new(big.Rat).SetString(b)
		if !oka || !okb {
			return // exponent too large for big.Rat
		}
		if (canonNumber(a) == canonNumber(b)) != (ra.Cmp(rb) == 0) {
			t.Fatalf("%s -> %s, %s -> %s; exact equal %v", a, canonNumber(a), b, canonNumber(b), ra.Cmp(rb) == 0)
		}
		if !isNumber(canonNumber(a)) {
			t.Fatalf("%s -> %s: not a JSON number", a, canonNumber(a))
		}
	})
}

func isNumber(s string) bool {
	var n json.Number
	return json.Unmarshal([]byte(s), &n) == nil && json.Valid([]byte(s)) && s != "" && (s[0] == '-' || (s[0] >= '0' && s[0] <= '9'))
}
