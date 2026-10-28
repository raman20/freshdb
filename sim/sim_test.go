package sim

import (
	"flag"
	"os"
	"strconv"
	"testing"

	"github.com/raman20/freshdb/store"
)

var (
	seedFlag  = flag.Int64("seed", -1, "run a single seed (reproduce a failure)")
	seedsFlag = flag.Int("seeds", 0, "number of seeds (default 500, or FRESHDB_SIM_SEEDS)")
)

func probs(seed int64) store.FaultProbs {
	// vary fault intensity per seed: some seeds are calm, some are storms
	lvl := float64(seed%5) / 4
	return store.FaultProbs{FailBefore: 0.04 * lvl, FailAfter: 0.04 * lvl, Delay: 0.04 * lvl, Dup: 0.03 * lvl, ReadFail: 0.03 * lvl, Torn: 0.03 * lvl}
}

func TestSimulation(t *testing.T) {
	n := 500
	if s := os.Getenv("FRESHDB_SIM_SEEDS"); s != "" {
		n, _ = strconv.Atoi(s)
	}
	if *seedsFlag > 0 {
		n = *seedsFlag
	}
	first := int64(1)
	if *seedFlag >= 0 {
		first, n = *seedFlag, 1
	}
	if testing.Short() && *seedFlag < 0 {
		n = 50
	}
	var tot Result
	for seed := first; seed < first+int64(n); seed++ {
		r, err := Run(seed, Config{Steps: 60, Probs: probs(seed)})
		if err != nil {
			t.Fatalf("%v\nreproduce: go test ./sim -run TestSimulation -seed=%d", err, seed)
		}
		tot.Acked += r.Acked
		tot.Ambiguous += r.Ambiguous
		tot.Crashes += r.Crashes
		tot.Closes += r.Closes
		tot.ANNRestores += r.ANNRestores
		tot.Takeovers += r.Takeovers
		tot.Shreds += r.Shreds
		tot.Rewinds += r.Rewinds
		tot.Compacts += r.Compacts
		tot.GCs += r.GCs
		tot.Faults += r.Faults
	}
	t.Logf("%d seeds OK: %d acked txns, %d ambiguous, %d crashes (%d clean closes, %d ann restores), %d takeovers, %d shreds, %d rewinds, %d compactions (%d gc), %d injected store faults",
		n, tot.Acked, tot.Ambiguous, tot.Crashes, tot.Closes, tot.ANNRestores, tot.Takeovers, tot.Shreds, tot.Rewinds, tot.Compacts, tot.GCs, tot.Faults)
}

// TestDeterminism: the same seed produces a bit-identical trace.
func TestDeterminism(t *testing.T) {
	for seed := int64(1); seed <= 40; seed++ {
		a, err := Run(seed, Config{Steps: 60, Probs: probs(seed)})
		if err != nil {
			t.Fatal(err)
		}
		b, _ := Run(seed, Config{Steps: 60, Probs: probs(seed)})
		if a.TraceHash != b.TraceHash {
			t.Fatalf("seed %d is not deterministic", seed)
		}
	}
}
