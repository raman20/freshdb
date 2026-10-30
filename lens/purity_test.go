package lens

import (
	"go/parser"
	"go/token"
	"os"
	"strconv"
	"strings"
	"testing"
)

// TestPurityImports enforces "lens = pure fold over the log": non-test lens
// code may not import anything that could observe the outside world.
func TestPurityImports(t *testing.T) {
	allowed := map[string]bool{
		"bytes": true, "crypto/sha256": true, "encoding/binary": true, "encoding/json": true,
		"errors": true, "hash/crc32": true, "math": true, "slices": true, "sort": true, "strconv": true, "strings": true,
		"github.com/raman20/freshdb/format": true,
	}
	ents, _ := os.ReadDir(".")
	for _, e := range ents {
		n := e.Name()
		if !strings.HasSuffix(n, ".go") || strings.HasSuffix(n, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(token.NewFileSet(), n, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, im := range f.Imports {
			p, _ := strconv.Unquote(im.Path.Value)
			if !allowed[p] {
				t.Errorf("%s imports %q: lenses must be pure (no clocks, randomness, IO, concurrency)", n, p)
			}
		}
	}
}
