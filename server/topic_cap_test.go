package server

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/raman20/freshdb"
)

// TestTopicReadCapped: C-D3. A topic read without max (or with max=0 or a
// huge max) returned the whole topic in one response, and a group fetch
// with max=0 acknowledged the whole topic. Both stop at maxMessages now.
func TestTopicReadCapped(t *testing.T) {
	db, ts := pushServer(t)
	n := maxMessages + 200
	err := db.Update(context.Background(), func(tx *freshdb.Tx) error {
		for i := 0; i < n; i++ {
			if err := tx.Topic("t").Publish([]byte(fmt.Sprint(i))); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{"", "?max=0", "?max=18446744073709551615"} {
		if ms, _ := getMsgs(t, "GET", ts.URL+"/v1/topics/t"+q); len(ms) != maxMessages {
			t.Errorf("GET %q: %d messages, want %d", q, len(ms), maxMessages)
		}
	}
	if ms, _ := getMsgs(t, "GET", ts.URL+"/v1/topics/t?max=5"); len(ms) != 5 {
		t.Errorf("max=5: %d messages", len(ms))
	}
	if ms, _ := getMsgs(t, "POST", ts.URL+"/v1/topics/t/groups/g/next?max=0"); len(ms) != maxMessages {
		t.Errorf("next max=0: %d messages", len(ms))
	}
	if ms, _ := getMsgs(t, "POST", ts.URL+"/v1/topics/t/groups/g/next"); len(ms) != 10 || ms[0].Offset != uint64(maxMessages) {
		t.Errorf("next: %d messages from %v", len(ms), ms)
	}
	req, _ := http.NewRequest("POST", ts.URL+"/v1/topics/t/groups/a%00b/next", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Errorf("group name with NUL: %d, want 400", resp.StatusCode)
	}
}

// TestCompactTopicRetention: the compact and gc endpoints take the topic
// retention options; bad values are 400.
func TestCompactTopicRetention(t *testing.T) {
	db, ts := pushServer(t)
	for i := 0; i < 10; i++ {
		db.Update(context.Background(), func(tx *freshdb.Tx) error { return tx.Topic("t").Publish([]byte{1}) })
	}
	post := func(q string) int {
		t.Helper()
		resp, err := http.Post(ts.URL+q, "", nil)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	for _, q := range []string{"/v1/compact?topic-max-age=x", "/v1/compact?topic-max-messages=-1", "/v1/gc?topic-ignore-groups=maybe"} {
		if c := post(q); c != 400 {
			t.Errorf("%s: %d, want 400", q, c)
		}
	}
	if c := post("/v1/compact?grace=0s&topic-max-messages=4&topic-ignore-groups=true&topic-max-age=1000h"); c != 200 {
		t.Fatalf("compact: %d", c)
	}
	if c := post("/v1/gc?grace=0s&topic-max-messages=2"); c != 200 {
		t.Fatalf("gc: %d", c)
	}
	db.View(context.Background(), func(tx *freshdb.Tx) error {
		if s, n := tx.Topic("t").Start(), tx.Topic("t").Len(); s != 6 || n != 10 { // gc cut nothing new: no new base
			t.Errorf("start %d len %d, want 6 10", s, n)
		}
		return nil
	})
}
