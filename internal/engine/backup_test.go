package engine

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// TestBackup backs up an engine that traded and stopped, and restores it into an empty folder. The
// restored journals replay to the snapshot's books, and an engine started there has the same books.
func TestBackup(t *testing.T) {
	dir, backup, restored := t.TempDir(), t.TempDir(), t.TempDir()
	live, err := New(dir, instruments...)
	if err != nil {
		t.Fatal(err)
	}
	live.every = 500
	trade(t, live)
	if _, err := Backup(dir, backup); err != nil {
		t.Fatal(err)
	}
	if _, err := Restore(backup, restored); err != nil {
		t.Fatal(err)
	}
	c, err := Check(restored, instruments...)
	if err != nil {
		t.Fatal(err)
	}
	if c.Inputs != live.input || c.Journals < 2 {
		t.Fatalf("replayed %d inputs from %d journals, want %d from more", c.Inputs, c.Journals, live.input)
	}
	again, err := New(restored, instruments...)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(books(again), books(live)) {
		t.Fatal("the restored books differ from the live ones")
	}

	// One byte changed in the backup, and the restore refuses it.
	journals, _ := filepath.Glob(filepath.Join(backup, journalFile+".*"))
	b, _ := os.ReadFile(journals[0])
	b[len(b)/2] ^= 1
	os.WriteFile(journals[0], b, 0o644)
	_, err = Restore(backup, t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "doesn't match its checksum") {
		t.Fatalf("a damaged backup restored with %v", err)
	}
}
