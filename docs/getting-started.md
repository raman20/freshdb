# Getting started

This walks through every lens from the CLI, then the same engine from Go.
All commands and outputs below are from a real run of unreleased `main` on
Linux (LSNs depend on what you ran before; hashes differ every run).

## Install

The latest release is v0.1.0, which predates the journal layout and
`freshdb migrate`. To get what these docs describe, install `main`:

```sh
go install github.com/raman20/freshdb/cmd/freshdb@main     # needs Go 1.27+
freshdb version
```

or build from a clone:

```sh
git clone https://github.com/raman20/freshdb && cd freshdb
go build ./cmd/freshdb && ./freshdb version
```

## Where the data goes

Every command takes `--db URL`, or reads `FRESHDB_URL`, and defaults to
`./freshdb-data`. A new local directory gets the journal layout on
Linux/macOS/BSD (one file, `journal.fj`). See [operations.md](operations.md)
for every URL form.

```console
$ export FRESHDB_URL=./gs
```

Without `--addr` each CLI call opens the database itself (embedded mode),
which takes the write lease. That is fine for trying things out; for
anything long-running, use `freshdb serve` (below).

## KV

```console
$ freshdb put config/model small
{"lsn":1}
$ freshdb put config/temp 0.2
{"lsn":2}
$ freshdb scan config/
[{"key":"config/model","value":"small"},{"key":"config/temp","value":"0.2"}]
$ freshdb del config/temp
{"lsn":3}
$ freshdb scan config/
[{"key":"config/model","value":"small"}]
$ freshdb get config/temp --at 2              # time travel: as of LSN 2
0.2
```

## Documents and indexes

```console
$ freshdb doc put memories m1 '{"user":"alice","text":"likes tea"}' --subject alice
{"lsn":4}
$ freshdb doc index memories user              # built by replaying the log
{"ok":true}
$ freshdb doc find memories user alice
["m1"]
$ freshdb doc get memories m1
{"user":"alice","text":"likes tea"}
```

`--subject alice` encrypts the value under Alice's key so it can be
shredded later.

## Vectors

Cosine similarity, exact search.

```console
$ freshdb vec put mem m1 0.9,0.1,0 --subject alice
{"lsn":5}
$ freshdb vec put mem m2 0,1,0
{"lsn":6}
$ freshdb vec search mem 1,0,0 -k 2
[{"id":"m1","score":0.9938837},{"id":"m2","score":0}]
```

## Topics and exactly-once processing

```console
$ freshdb pub tasks "summarise inbox"
{"lsn":7}
$ freshdb sub tasks --from 0 --max 1           # read without a group
[{"lsn":7,"offset":0,"value":"summarise inbox"}]
```

To process a message exactly once, write the result and ack the group
cursor in **one** transaction:

```console
$ freshdb tx '[{"op":"put","key":"result/t0","value":"3 unread"},{"op":"ack","topic":"tasks","group":"worker","offset":0}]'
{"lsn":8,"ops":2}
$ freshdb sub tasks --group worker             # nothing left for this group
[]
$ freshdb get result/t0
3 unread
```

`freshdb sub --group G` on its own fetches *and* acks, so it is at-most-once
for the client. `tx` ops: `put`, `del`, `pub`, `ack`, `doc`, `vec`.
`sub --follow` keeps polling for new messages.

## Forget a subject

```console
$ freshdb shred alice                          # prints a signed certificate
{ "certificate": { ... "dek_deleted": true, ... "shred_lsn": 9, ... }, "valid_signature": true }
$ freshdb doc find memories user alice
[]
$ freshdb vec search mem 1,0,0 -k 2            # m1 was Alice's
[{"id":"m2","score":0}]
$ freshdb verify
{ ..., "ok": true, ... }
```

`freshdb log [--from LSN]` prints the raw log. Subject-tagged values show
only as `<encrypted N bytes, sha256 …>`, and after a shred nothing can
decrypt them.

## Branches, rewind, promote

```console
$ freshdb branch create try --at 2             # fork main as of LSN 2; copies nothing
{"heads":{"main":9,"try":2},"ok":true}
$ freshdb get config/temp --branch try
0.2
$ freshdb branch list                          # refs, heads and the reflog
$ freshdb rewind main 1                        # main back to LSN 1; old head kept in the reflog
{"heads":{"main":1,"try":2},"ok":true}
```

`branch promote SRC DST` points `DST` at `SRC`'s log; it only succeeds as a
fast-forward (`409 promote is not a fast-forward` otherwise).

## Server mode

```sh
freshdb serve --db ./gs --listen 127.0.0.1:7070 &
export FRESHDB_ADDR=http://127.0.0.1:7070       # the CLI now talks to the server
freshdb put k v
curl -s localhost:7070/v1/kv/k                  # v
curl -s -X PUT localhost:7070/v1/kv/k2 -d v2    # {"lsn":...}
```

The endpoint list is in the [README](../README.md#api-tour). The server has
**no auth or TLS**; keep it on `127.0.0.1` (the default).

## From Go

```sh
go get github.com/raman20/freshdb@main
```

[`examples/tour/main.go`](../examples/tour/main.go) uses every lens, time
travel, branches, rewind, shred and verify on `mem://`. From a clone:
`go run ./examples/tour`.

```go
db, err := freshdb.Open(ctx, "./data")       // or "s3://bucket/prefix?endpoint=...", "mem://x"
if err != nil { ... }
defer db.Close()
db.Put(ctx, "config/model", []byte("small"))
```

Options: `WithValueCacheBytes`, `WithInlineMax`, `WithKEK`,
`WithSnapshotEvery`, `WithLogf` (see [operations.md](operations.md) and
[pkg.go.dev](https://pkg.go.dev/github.com/raman20/freshdb)).

## The agent demo

[`examples/agent-demo.sh`](../examples/agent-demo.sh) runs a server and walks
an AI agent through memory, exactly-once tasks, undo, a branch plan and
forgetting. It needs `jq` and `curl`:

```sh
go build -o /tmp/freshdb ./cmd/freshdb && FRESHDB=/tmp/freshdb ./examples/agent-demo.sh
```

It writes `./agent-demo-data` in the current directory.
