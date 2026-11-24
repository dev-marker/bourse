package engine

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/fiston-user/bourse/internal/book"
	"github.com/fiston-user/bourse/internal/journal"
	"github.com/fiston-user/bourse/internal/market"
)

// Follower reads an engine's journal while the engine writes it, from the first input on, across
// rotations. Every input goes through books of its own, with the engine's own code, so it hands back
// exactly the events the engine produced. It only reads inputs the engine has synced.
type Follower struct {
	dir     string
	symbols map[string]*symbol
	input   uint64 // the last input read
	synced  uint64 // the last input the engine said was on disk
	file    *os.File
	r       *journal.Reader
}

// Follow returns a follower for the engine with its journal in dir, trading these instruments.
func Follow(dir string, instruments ...market.Instrument) *Follower {
	f := &Follower{dir: dir, symbols: map[string]*symbol{}}
	for _, in := range instruments {
		f.symbols[in.Symbol] = &symbol{book: book.New(in)}
	}
	return f
}

// Next waits for the next input the engine has synced, and returns it with the events it caused.
func (f *Follower) Next(ctx context.Context) (journal.Entry, Report, error) {
	for !f.Behind() {
		select {
		case <-ctx.Done():
			return journal.Entry{}, Report{}, ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
	entry, err := f.read()
	if err == nil && entry.Seq != f.input+1 {
		err = fmt.Errorf("the journal jumps from input %d to %d", f.input, entry.Seq)
	}
	s, ok := f.symbols[entry.Symbol]
	if err == nil && !ok {
		err = fmt.Errorf("input %d is for %s, which isn't listed", entry.Seq, entry.Symbol)
	}
	if err != nil {
		return journal.Entry{}, Report{}, err
	}
	f.input = entry.Seq
	return entry, s.apply(entry.Cancel, entry.Order), nil
}

// Behind reports whether the engine has synced inputs the follower hasn't read yet.
func (f *Follower) Behind() bool {
	if f.input == f.synced {
		b, err := os.ReadFile(filepath.Join(f.dir, syncedFile))
		if err == nil && len(b) == 8 {
			f.synced = binary.LittleEndian.Uint64(b)
		}
	}
	return f.input < f.synced
}

// read returns the next entry. At the end of a file, the engine has rotated it away, because the next
// input is on disk: it's in the next file.
func (f *Follower) read() (journal.Entry, error) {
	for {
		if f.r == nil {
			if err := f.open(); err != nil {
				return journal.Entry{}, err
			}
		}
		e, err := f.r.Next()
		if err != io.EOF {
			return e, err
		}
		f.file.Close()
		f.r = nil
	}
}

// open opens the file with the next input in it. A rotated journal is named after its last input, so
// it's the first one named after where we are, or else the journal the engine is writing now.
func (f *Follower) open() error {
	path := filepath.Join(f.dir, journalFile)
	file, err := os.Open(path) // before looking for rotated ones, so a rotation can't slip between
	if err != nil {
		return err
	}
	rotated, err := filepath.Glob(path + ".*")
	if err != nil {
		file.Close()
		return err
	}
	next := uint64(math.MaxUint64)
	for _, name := range rotated {
		last, err := strconv.ParseUint(strings.TrimPrefix(name, path+"."), 10, 64)
		if err == nil && last > f.input {
			next = min(next, last)
		}
	}
	if next != math.MaxUint64 {
		file.Close()
		if file, err = os.Open(fmt.Sprintf("%s.%d", path, next)); err != nil {
			return err
		}
	}
	f.file, f.r = file, journal.NewReader(file)
	return nil
}

// Close closes the journal file the follower is reading.
func (f *Follower) Close() error {
	if f.file == nil {
		return nil
	}
	return f.file.Close()
}
