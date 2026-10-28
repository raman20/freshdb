package freshdb

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/raman20/freshdb/format"
	"github.com/raman20/freshdb/lens"
	"github.com/raman20/freshdb/store"
)

// Certificate is a signed record that a subject was crypto-shredded.
type Certificate struct {
	SubjectHash string   `json:"subject_hash"`
	ShredLSN    uint64   `json:"shred_lsn"`
	Branch      string   `json:"branch"`
	ChainHead   string   `json:"chain_head"` // hash-chain head after the SHRED record
	ManifestID  uint64   `json:"manifest_id"`
	DEKDeleted  bool     `json:"dek_deleted"`
	Lenses      []string `json:"lenses_purged"`
	TS          int64    `json:"ts"`
	PublicKey   string   `json:"public_key"`
	Signature   string   `json:"signature,omitempty"`
}

func (c *Certificate) signingBytes() []byte {
	cc := *c
	cc.Signature = ""
	b, _ := json.Marshal(cc)
	return b
}

// VerifyCertificate checks the certificate signature.
func VerifyCertificate(c *Certificate) bool {
	pk, err := hex.DecodeString(c.PublicKey)
	sig, err2 := hex.DecodeString(c.Signature)
	if err != nil || err2 != nil || len(pk) != ed25519.PublicKeySize {
		return false
	}
	return ed25519.Verify(pk, c.signingBytes(), sig)
}

// Shred crypto-shreds a subject: its DEK is destroyed, every lens on every
// open branch drops the subject's data, future lens builds redact it, a
// SHRED record is appended to main (the hash chain stays verifiable because
// it covers ciphertext digests), and a signed certificate is returned.
func (db *DB) Shred(ctx context.Context, subject string) (*Certificate, error) {
	h := SubjectHash(subject)
	k := db.keys
	marker := fmt.Sprintf(`{"subject_hash":%q,"ts":%d}`, h, db.opts.Clock())
	if err := putExactlyOnce(ctx, db.st, "keys/shredded/"+h, []byte(marker)); err != nil && !errors.Is(err, store.ErrExists) {
		return nil, err
	}
	k.mu.Lock()
	k.shredded[h] = true
	delete(k.deks, h)
	k.mu.Unlock()
	if err := db.st.Delete(ctx, "keys/dek/"+h); err != nil {
		return nil, err
	}
	db.mu.Lock()
	var names []string
	for _, id := range sortedMapKeys(db.views) {
		v := db.views[id]
		v.mu.Lock()
		for _, l := range v.lenses() {
			l.DropSubject(h)
		}
		v.mu.Unlock()
	}
	for _, l := range db.views[db.man.Refs["main"]].lenses() {
		names = append(names, l.Name())
	}
	// Saved HNSW graphs hold a hash per vector and a link structure built
	// with the shredded vectors present: delete them, and save none built
	// before now (the next open or compaction rebuilds from scratch).
	db.annTainted = true
	ids := map[string]bool{}
	for id := range db.man.Logs {
		ids[id] = true
	}
	for _, g := range db.man.Garbage {
		ids[g.Log] = true
	}
	for _, id := range sortedMapKeys(ids) {
		if _, err := db.dropANN(ctx, id); err != nil {
			db.mu.Unlock()
			return nil, err
		}
	}
	db.mu.Unlock()
	lsn, err := db.Branch("main").UpdateLSN(ctx, func(tx *Tx) error {
		tx.ops = append(tx.ops, format.Op{Kind: format.OpShred, Space: "__audit", Key: h})
		return nil
	})
	if err != nil {
		return nil, err
	}
	w, err := db.branchWriter("main")
	if err != nil {
		return nil, err
	}
	ch, err := w.l.chainAt(ctx, lsn)
	if err != nil {
		return nil, err
	}
	db.mu.Lock()
	mid := db.man.ID
	db.mu.Unlock()
	c := &Certificate{SubjectHash: h, ShredLSN: lsn, Branch: "main", ChainHead: hex.EncodeToString(ch[:]), ManifestID: mid,
		DEKDeleted: true, Lenses: names, TS: db.opts.Clock(), PublicKey: hex.EncodeToString(k.sign.Public().(ed25519.PublicKey))}
	c.Signature = hex.EncodeToString(ed25519.Sign(k.sign, c.signingBytes()))
	b, _ := json.Marshal(c)
	key := fmt.Sprintf("keys/certs/%s-%d.json", h, lsn)
	if err := putExactlyOnce(ctx, db.st, key, b); err != nil && !errors.Is(err, store.ErrExists) {
		return c, err
	}
	return c, nil
}

// IsShredded reports whether a subject has been shredded.
func (db *DB) IsShredded(subject string) bool { return db.keys.isShredded(SubjectHash(subject)) }

// ---- Verify ----

// VerifyReport is the result of Verify.
type VerifyReport struct {
	OK              bool              `json:"ok"`
	Manifests       int               `json:"manifests"`
	Logs            int               `json:"logs"`
	Segments        int               `json:"segments"`
	Entries         int               `json:"entries"`
	ChainHeads      map[string]string `json:"chain_heads"`
	EncryptedValues int               `json:"encrypted_values"`
	Unreadable      int               `json:"unreadable_shredded_values"`
	SweptDEKs       int               `json:"swept_late_deks,omitempty"`
	Problems        []string          `json:"problems,omitempty"`
}

// Verify re-reads everything from storage and checks manifest monotonicity
// (dense ids, non-decreasing epochs), segment CRCs, LSN density, writer
// epoch monotonicity, hash-chain continuity across branches, AEAD tags of
// every non-shredded encrypted value, and that no shredded subject still
// has a DEK.
func (db *DB) Verify(ctx context.Context) (*VerifyReport, error) {
	r := &VerifyReport{ChainHeads: map[string]string{}}
	bad := func(f string, a ...any) { r.Problems = append(r.Problems, fmt.Sprintf(f, a...)) }
	keys, err := db.st.List(ctx, "manifest/")
	if err != nil {
		return nil, err
	}
	// GC deletes old manifests and a fenced writer may leave a stray one
	// below the latest (putManifest), so only the dense run of ids ending
	// at the latest is checked.
	var ids []uint64
	for _, k := range keys {
		if n, ok := parseNum(k, "manifest/", ".mf"); ok {
			ids = append(ids, n)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	start := len(ids) - 1
	for start > 0 && ids[start-1] == ids[start]-1 {
		start--
	}
	keys = keys[:0]
	for _, n := range ids[max(start, 0):] {
		keys = append(keys, manifestKey(n))
	}
	var prevEpoch, prevID uint64
	var last *Manifest
	for _, k := range keys {
		b, err := db.st.Get(ctx, k)
		if err != nil {
			return nil, err
		}
		m, err := decodeManifest(b)
		if err != nil {
			bad("%s: %v", k, err)
			continue
		}
		if prevID != 0 && m.ID != prevID+1 {
			bad("manifest ids not dense: %d after %d", m.ID, prevID)
		}
		if m.Epoch < prevEpoch {
			bad("manifest %d epoch went backwards", m.ID)
		}
		prevID, prevEpoch, last = m.ID, m.Epoch, m
		r.Manifests++
	}
	if last == nil {
		return nil, errors.New("freshdb: no manifest")
	}
	fresh := &DB{st: db.st, opts: db.opts, epoch: db.epoch, keys: db.keys, env: db.env, man: last, logs: map[string]*logState{}}
	for _, id := range sortedMapKeys(last.Logs) {
		l, err := fresh.loadLog(ctx, id, false)
		if err != nil {
			bad("log %s: %v", id, err)
			continue
		}
		r.Logs++
		if l.meta.Base != nil {
			if _, err := fresh.loadBase(ctx, l); err != nil {
				bad("log %s: %v", id, err)
			}
		}
		segs, _ := db.st.List(ctx, "log/"+id+"/")
		r.Segments += len(segs)
		err = l.eachOwn(ctx, l.head(), func(e *lens.Entry) error { // the parent is verified on its own
			r.Entries++
			for i, op := range e.Ops {
				if op.Subject == "" || op.Kind == format.OpDel || op.Kind == format.OpShred {
					continue
				}
				r.EncryptedValues++
				if db.keys.isShredded(op.Subject) {
					r.Unreadable++
					continue
				}
				if _, ok := db.env.Open(e, i); !ok {
					bad("log %s LSN %d op %d: AEAD check failed", id, e.LSN, i)
				}
			}
			return nil
		})
		if err != nil {
			bad("log %s: %v", id, err)
		}
	}
	for name, id := range last.Refs {
		if l := fresh.logs[id]; l != nil {
			h := l.head()
			c, err := l.chainAt(ctx, h)
			if err != nil {
				bad("log %s: %v", id, err)
			}
			r.ChainHeads[name] = fmt.Sprintf("%d:%s", h, hex.EncodeToString(c[:8]))
		}
	}
	deks, _ := db.st.List(ctx, "keys/dek/")
	for _, d := range deks {
		if db.keys.isShredded(d[strings.LastIndex(d, "/")+1:]) {
			// A DEK-create request that was still in flight when the subject
			// was shredded can land afterwards (object stores cannot cancel
			// it). The shred marker already blocks every read; sweep it.
			if err := db.st.Delete(ctx, d); err != nil {
				bad("shredded subject still has a DEK: %s (%v)", d, err)
			} else {
				r.SweptDEKs++
			}
		}
	}
	r.OK = len(r.Problems) == 0
	return r, nil
}

// PublicKey returns the hex ed25519 key that signs deletion certificates.
func (db *DB) PublicKey() string {
	return hex.EncodeToString(db.keys.sign.Public().(ed25519.PublicKey))
}
