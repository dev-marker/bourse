package engine

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"hash/crc32"
	"io/fs"
	"os"
	"path/filepath"
	"slices"

	"github.com/fiston-user/bourse/internal/journal"
	"github.com/fiston-user/bourse/internal/market"
)

// snapshot is every book after one input: enough to start again from there.
type snapshot struct {
	Input uint64 // the last input it covers
	Books []bookState
}

// bookState is one symbol's book, and the numbers its goroutine hands out.
type bookState struct {
	Symbol string
	Seq    uint64         // the last event sequence number
	ID     uint64         // the last order ID
	Orders []market.Order // every resting order, in the order to add them back
}

// state is the symbol's book and numbers, right now. Only its goroutine may call it.
func (s *symbol) state() bookState {
	return bookState{Symbol: s.book.Instrument.Symbol, Seq: s.seq, ID: s.id, Orders: s.book.Orders()}
}

// takeSnapshot asks every book for its state, writes it all down, and starts a new journal. Only record
// calls it, between two batches, so every input in the journal is already in its book's inbox.
func (e *Engine) takeSnapshot() error {
	states := make(chan bookState, len(e.symbols))
	for _, s := range e.symbols {
		s.inbox <- command{state: states}
	}
	snap := snapshot{Input: e.input}
	for range e.symbols {
		snap.Books = append(snap.Books, <-states)
	}
	// The books answer in any order. Sorted, the same books always make the same file.
	slices.SortFunc(snap.Books, func(a, b bookState) int { return cmp.Compare(a.Symbol, b.Symbol) })
	if err := writeSnapshot(filepath.Join(e.dir, snapshotFile), &snap); err != nil {
		return err
	}
	e.snapped = snap.Input
	return e.rotate()
}

// writeSnapshot writes snap to path, whole or not at all: a crash leaves the old one or the new one.
func writeSnapshot(path string, snap *snapshot) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(snap); err != nil {
		return err
	}
	buf.Write(binary.LittleEndian.AppendUint32(nil, crc32.Checksum(buf.Bytes(), castagnoli)))
	f, err := os.Create(path + ".tmp")
	if err != nil {
		return err
	}
	_, err = f.Write(buf.Bytes())
	if err == nil {
		err = f.Sync()
	}
	if err = errors.Join(err, f.Close()); err != nil {
		return err
	}
	if err := os.Rename(f.Name(), path); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

// castagnoli is the table for CRC-32C, the same checksum the journal uses.
var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// syncDir waits until the disk has dir's list of files.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// rotate keeps the journal so far under a name of its own, and starts an empty one. The snapshot
// covers everything in the old one, so recovery never needs it again.
func (e *Engine) rotate() error {
	if err := e.journal.Close(); err != nil {
		return err
	}
	path := filepath.Join(e.dir, journalFile)
	if err := os.Rename(path, fmt.Sprintf("%s.%d", path, e.input)); err != nil {
		return err
	}
	j, err := journal.Create(path)
	if err != nil {
		return err
	}
	e.journal = j
	return nil
}

// restore loads the snapshot in the engine's folder, if there is one, into the empty books.
func (e *Engine) restore() error {
	b, err := os.ReadFile(filepath.Join(e.dir, snapshotFile))
	if errors.Is(err, fs.ErrNotExist) {
		return nil // a first start: everything is in the journal
	}
	if err != nil {
		return err
	}
	n := len(b) - 4
	if n < 0 || crc32.Checksum(b[:n], castagnoli) != binary.LittleEndian.Uint32(b[n:]) {
		return errors.New("the snapshot is damaged")
	}
	var snap snapshot
	if err := gob.NewDecoder(bytes.NewReader(b[:n])).Decode(&snap); err != nil {
		return err
	}
	for _, st := range snap.Books {
		s, ok := e.symbols[st.Symbol]
		if !ok {
			return fmt.Errorf("the snapshot has a book for %s, which this engine doesn't trade", st.Symbol)
		}
		s.seq, s.id = st.Seq, st.ID
		for _, o := range st.Orders {
			if err := s.book.Add(o); err != nil {
				return err
			}
		}
	}
	e.input, e.snapped, e.startup.Snapshot = snap.Input, snap.Input, snap.Input
	return nil
}
