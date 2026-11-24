package engine

import (
	"maps"
	"path/filepath"
	"slices"
	"testing"
)

// TestFollow trades a random workload, with a snapshot and a new journal every 300 inputs. A follower
// reads the journals afterwards, and must hand back exactly the events the clients got from the engine.
func TestFollow(t *testing.T) {
	dir := t.TempDir()
	live, err := New(dir, instruments...)
	if err != nil {
		t.Fatal(err)
	}
	live.every = 300
	events := trade(t, live)
	if rotated, _ := filepath.Glob(filepath.Join(dir, journalFile+".*")); len(rotated) < 2 {
		t.Fatalf("%d rotated journals, want a few", len(rotated))
	}

	f := Follow(dir, instruments...)
	defer f.Close()
	followed := map[string][]Event{}
	for f.Behind() {
		entry, report, err := f.Next(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		followed[entry.Symbol] = append(followed[entry.Symbol], report.Events...)
	}
	if f.input != live.input {
		t.Fatalf("the follower stopped at input %d, the engine wrote %d", f.input, live.input)
	}
	if !maps.EqualFunc(followed, events, slices.Equal[[]Event]) {
		t.Fatal("the followed events differ from the engine's")
	}
}
