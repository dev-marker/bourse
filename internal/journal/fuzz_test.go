package journal_test

import (
	"testing"

	"github.com/fiston-user/bourse/internal/journal"
)

// FuzzEntry decodes random bytes. It must never panic, and whatever it accepts must encode and decode
// back to the same entry.
func FuzzEntry(f *testing.F) {
	for _, e := range entries {
		b, _ := e.AppendBinary(nil)
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		var e journal.Entry
		if e.UnmarshalBinary(data) != nil {
			return
		}
		b, err := e.AppendBinary(nil)
		if err != nil {
			t.Fatal(err)
		}
		var again journal.Entry
		if err := again.UnmarshalBinary(b); err != nil || again != e {
			t.Fatalf("decoded %+v, encoded it, and got back %+v, %v", e, again, err)
		}
	})
}
