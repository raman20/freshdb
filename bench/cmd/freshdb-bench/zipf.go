package main

import (
	"math"
	"math/rand/v2"
)

// zipfian is YCSB's ZipfianGenerator (Gray et al., "Quickly generating
// billion-record synthetic databases"), theta = 0.99 by default.
type zipfian struct {
	n                        uint64
	theta, alpha, zetan, eta float64
	half                     float64
}

func zeta(n uint64, theta float64) float64 {
	s := 0.0
	for i := uint64(1); i <= n; i++ {
		s += 1 / math.Pow(float64(i), theta)
	}
	return s
}

func newZipfian(n uint64, theta float64) *zipfian {
	z := &zipfian{n: n, theta: theta, alpha: 1 / (1 - theta), zetan: zeta(n, theta)}
	zeta2 := zeta(2, theta)
	z.eta = (1 - math.Pow(2/float64(n), 1-theta)) / (1 - zeta2/z.zetan)
	z.half = 1 + math.Pow(0.5, theta)
	return z
}

// next returns a rank in [0, n): 0 is the most popular.
func (z *zipfian) next(r *rand.Rand) uint64 {
	u := r.Float64()
	uz := u * z.zetan
	if uz < 1 {
		return 0
	}
	if uz < z.half {
		return 1
	}
	v := uint64(float64(z.n) * math.Pow(z.eta*u-z.eta+1, z.alpha))
	if v >= z.n {
		v = z.n - 1
	}
	return v
}

// scrambled spreads popular ranks over the key space (YCSB ScrambledZipfian:
// FNV-1a 64 of the rank, modulo n).
func scrambled(rank, n uint64) uint64 {
	h := uint64(14695981039346656037)
	for i := 0; i < 8; i++ {
		h ^= rank & 0xff
		h *= 1099511628211
		rank >>= 8
	}
	return h % n
}
