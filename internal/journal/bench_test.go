//go:build unix

package journal_test

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/fiston-user/bourse/internal/journal"
)

// BenchmarkSync writes a small record and syncs it, with a plain fsync and with Go's File.Sync.
func BenchmarkSync(b *testing.B) {
	syncs := []struct {
		name string
		sync func(f *os.File) error
	}{
		{"fsync", func(f *os.File) error { return syscall.Fsync(int(f.Fd())) }},
		{"File.Sync", (*os.File).Sync},
	}
	for _, s := range syncs {
		b.Run(s.name, func(b *testing.B) {
			f, err := os.Create(filepath.Join(b.TempDir(), "sync"))
			if err != nil {
				b.Fatal(err)
			}
			defer f.Close()
			record := make([]byte, 60)
			for b.Loop() {
				f.Write(record)
				s.sync(f)
			}
		})
	}
}

// BenchmarkAppend appends entries and syncs after every batch of them, so a batch shares one sync.
func BenchmarkAppend(b *testing.B) {
	for _, batch := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprint("batch=", batch), func(b *testing.B) {
			w, err := journal.Create(filepath.Join(b.TempDir(), "engine.journal"))
			if err != nil {
				b.Fatal(err)
			}
			defer w.Close()
			e := entries[0]
			for i := 0; b.Loop(); i++ {
				w.Append(&e)
				if i%batch == batch-1 {
					w.Sync()
				}
			}
		})
	}
}
