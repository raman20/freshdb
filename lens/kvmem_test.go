package lens

import (
	"reflect"
	"testing"
	"unsafe"
)

func TestVerPacked(t *testing.T) {
	if s := unsafe.Sizeof(ver{}); s != 40 {
		t.Fatalf("sizeof(ver) = %d, want 40", s)
	}
	for _, v := range []Version{
		{LSN: 7, Off: 100, Len: 1024, Op: 2, Sum: 99, Chunk: 3},
		{LSN: 8, Val: []byte{}},
		{LSN: 9, Val: []byte("hi"), Subject: "alice"},
		{LSN: 10, Subject: "bob", Off: 5, Len: 6, Del: true},
	} {
		if got := pack(v).unpack(); !reflect.DeepEqual(got, v) {
			t.Errorf("round trip %+v -> %+v", v, got)
		}
	}
}

// DropSubject must redact versions in both the inline first slot and the
// later slice, and keep other subjects.
func TestKVDropSubjectPacked(t *testing.T) {
	l := NewKV()
	l.put(ck("kv", "a"), Version{LSN: 1, Val: []byte("1"), Subject: "s"})
	l.put(ck("kv", "a"), Version{LSN: 2, Val: []byte("2"), Subject: "t"})
	l.put(ck("kv", "a"), Version{LSN: 3, Val: []byte("3"), Subject: "s"})
	l.DropSubject("s")
	for at, want := range map[uint64]string{1: "", 2: "2", 3: ""} {
		v, ok := l.Get("kv", "a", at)
		if got := string(v.Val); ok != (want != "") || got != want {
			t.Errorf("at %d: %q %v, want %q", at, got, ok, want)
		}
	}
	if l.LastWrite("kv", "a") != 3 {
		t.Fatal("last write")
	}
}
