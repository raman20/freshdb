package lens

import (
	"fmt"
	"math/rand"
	"testing"
)

func BenchmarkHNSWBuild(b *testing.B) {
	r := rand.New(rand.NewSource(1))
	vs := make([][]float32, 20000)
	for i := range vs {
		vs[i], _ = normalize(randVec(r, 128))
	}
	for b.Loop() {
		l := NewVector()
		l.EnableANN("c", ANNParams{})
		for i, v := range vs {
			l.add("c", fmt.Sprint("v", i), "", v)
		}
	}
}
