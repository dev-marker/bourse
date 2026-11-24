package journal_test

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/fiston-user/bourse/internal/journal"
	"github.com/fiston-user/bourse/internal/market"
)

// entries are one of each kind: a limit buy, a market sell and a cancel. They take 60, 55 and 44 bytes.
var entries = []journal.Entry{
	{Seq: 1, Time: 1_764_000_000_000_000_001, Symbol: "ACME", Order: market.Order{Symbol: "ACME",
		Account: "alice", ClientID: "a-1", Side: market.Buy, Type: market.Limit, TIF: market.GTC,
		Price: 10_127, Qty: 300, PostOnly: true}},
	{Seq: 2, Time: 1_764_000_000_000_000_002, Symbol: "ACME", Order: market.Order{Symbol: "ACME",
		Account: "bob", Side: market.Sell, Type: market.Market, TIF: market.IOC, Qty: 100}},
	{Seq: 3, Time: 1_764_000_000_000_000_003, Symbol: "ACME", Cancel: 1,
		Order: market.Order{Account: "alice"}},
}

// write makes a journal with these entries, and returns its path.
func write(t *testing.T, entries []journal.Entry) string {
	path := filepath.Join(t.TempDir(), "engine.journal")
	w, err := journal.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if err := w.Append(&e); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

// read returns every entry it can read from the journal at path, and the error it stopped with.
func read(t *testing.T, path string) ([]journal.Entry, error) {
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	r := journal.NewReader(f)
	var got []journal.Entry
	for {
		e, err := r.Next()
		if err != nil {
			return got, err
		}
		got = append(got, e)
	}
}

// TestRoundTrip writes entries and reads them back, exactly as they were.
func TestRoundTrip(t *testing.T) {
	got, err := read(t, write(t, entries))
	if err != io.EOF || len(got) != len(entries) {
		t.Fatalf("read %d entries and then %v, want %d and EOF", len(got), err, len(entries))
	}
	for i := range got {
		if got[i] != entries[i] {
			t.Errorf("entry %d:\n got %+v\nwant %+v", i+1, got[i], entries[i])
		}
	}
}

// TestDamage damages a journal on disk, and expects the good entries before the damage, then an error.
func TestDamage(t *testing.T) {
	tests := []struct {
		name   string
		damage func(b []byte) []byte
		good   int // the entries that still read back
		want   error
	}{
		{"last record cut short", func(b []byte) []byte { return b[:len(b)-3] }, 2, journal.ErrTorn},
		{"cut inside a header", func(b []byte) []byte { return b[:119] }, 2, journal.ErrTorn},
		{"a flipped bit", func(b []byte) []byte { b[102] ^= 1; return b }, 1, journal.ErrCorrupt},
		{"zeros at the end", func(b []byte) []byte { return append(b, make([]byte, 16)...) },
			3, journal.ErrCorrupt},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := write(t, entries)
			b, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, tt.damage(b), 0o644); err != nil {
				t.Fatal(err)
			}
			got, err := read(t, path)
			if len(got) != tt.good || !errors.Is(err, tt.want) {
				t.Fatalf("read %d entries and then %v, want %d and %v", len(got), err, tt.good, tt.want)
			}
		})
	}
}

// TestOpen is a restart after a crash in the middle of a write. The last record is cut short, Open
// replays the two good entries, and the third is written again. Then all three must read back.
func TestOpen(t *testing.T) {
	path := write(t, entries)
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, b[:len(b)-3], 0o644); err != nil {
		t.Fatal(err)
	}
	var replayed []journal.Entry
	w, cut, err := journal.Open(path, func(e journal.Entry) error {
		replayed = append(replayed, e)
		return nil
	})
	if err != nil || len(replayed) != 2 {
		t.Fatalf("replayed %d entries and then %v, want 2 and no error", len(replayed), err)
	}
	if err := w.Append(&entries[2]); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	w.Close()
	got, err := read(t, path)
	if err != io.EOF || len(got) != 3 {
		t.Fatalf("read %d entries and then %v, want 3 and EOF", len(got), err)
	}
	if cut != 41 {
		t.Fatalf("cut %d bytes, want the 41 that were left of the last record", cut)
	}
}

// TestOpenDamaged expects Open to refuse a damaged journal, and to leave every byte of it alone.
func TestOpenDamaged(t *testing.T) {
	path := write(t, entries)
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	b[102] ^= 1
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
	_, _, err = journal.Open(path, func(journal.Entry) error { return nil })
	if !errors.Is(err, journal.ErrCorrupt) {
		t.Fatalf("Open said %v, want ErrCorrupt", err)
	}
	if after, _ := os.ReadFile(path); !bytes.Equal(after, b) {
		t.Fatal("Open changed a damaged journal")
	}
}
