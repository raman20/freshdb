# Writing a lens

A lens is a view over the log. freshDB ships four: KV (MVCC), topics with
consumer groups, documents with secondary indexes, and vectors. This guide
explains the contract every lens must satisfy. Read it before you change one
or propose a new one.

> v0.1.0 limitation: lenses are compiled in. There is no plugin API yet, so
> adding a lens means changing `lens/` and `view.go` in this repo.
> A public lens API is on the [roadmap](../ROADMAP.md).

## The contract

```go
type Lens interface {
    Name() string
    Apply(e *Entry, env Env)     // fold one committed transaction into the state
    DropSubject(subject string)  // forget everything a shredded subject contributed
    Digest() [32]byte            // hash of the full state, for equivalence checks
}
```

1. **Pure and deterministic.** `Apply` may only use the entry and `env`. No
   clocks (use `e.TS`), randomness, maps iterated in random order that leak
   into output, goroutines, files or network. `lens/purity_test.go` enforces
   an import allow-list.
2. **Fold equivalence.** For any log, the state after applying entries one by
   one must equal a fresh fold. Snapshot + tail must equal the full fold. The
   property tests in `lens/prop_test.go` check this with random logs.
3. **Shred equivalence.** `DropSubject(s)` must leave the lens *exactly* as if
   every op of `s` had been redacted from the start:
   `DropSubject(s) ∘ fold(log) == fold(redact(log, {s}))`.
   Watch out for derived structure: empty collections, counters, index
   entries and "last writer" fields. The vector lens once left an empty
   collection behind; a property test caught it.
4. **Canonical state.** `Digest` must hash a canonical encoding: sort keys,
   and make sure two equal states have identical bytes.
5. **Encrypted values.** Use `env.Open(e, i)` to read op `i`'s plaintext. It
   returns `ok=false` for shredded subjects; treat that op as if it were
   absent (or as a redacted placeholder, like topic offsets do).

## Checklist for a new lens

- [ ] Implementation in `lens/yourlens.go`, imports within the allow-list
- [ ] Property test: random entries → `fold == incremental`, `snapshot+tail == fold` (if it snapshots)
- [ ] Property test: `DropSubject == fold(redacted)`
- [ ] A reference model (the simplest correct implementation) and a refinement test against it
- [ ] Wire into `view.go` (`lenses()`, `buildView`) and expose it on `Tx`
- [ ] Run `go test ./lens ./sim` and the engine property test `TestPropEngineLensEqualsFoldAcrossRestart`
