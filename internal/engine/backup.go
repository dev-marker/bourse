package engine

import (
	"bufio"
	"bytes"
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/fiston-user/bourse/internal/book"
	"github.com/fiston-user/bourse/internal/journal"
	"github.com/fiston-user/bourse/internal/market"
)

// sumsFile lists every file in a backup with its SHA-256, the way sha256sum writes them.
const sumsFile = "SHA256SUMS"

// Backed is what a backup holds, and what was copied to make it.
type Backed struct {
	Snapshot uint64 // the last input the snapshot covers
	State    uint32 // a checksum of the snapshot's books: the same books give the same number
	Journals int    // sealed journals
	New      int    // journals copied this time
	Bytes    int64  // bytes copied this time, the snapshot's included
}

// Backup copies what a restore needs from the engine's folder to the folder to: the newest snapshot,
// and every sealed journal up to it, each with its checksum in SHA256SUMS. A sealed journal never
// changes, so one that's in the backup already isn't copied again. It's safe while the engine runs.
func Backup(dir, to string) (Backed, error) {
	var b Backed
	snap, err := readSnapshot(filepath.Join(dir, snapshotFile))
	if err != nil {
		return b, fmt.Errorf("the snapshot: %w", err)
	}
	encoded, err := encode(snap)
	if err != nil {
		return b, err
	}
	b.Snapshot, b.State = snap.Input, crc32.Checksum(encoded, castagnoli)
	journals, err := sealed(dir, snap.Input)
	if err != nil {
		return b, err
	}
	if err := os.MkdirAll(to, 0o755); err != nil {
		return b, err
	}
	sums, err := readSums(to)
	if err != nil {
		return b, err
	}
	for _, path := range append(journals, filepath.Join(dir, snapshotFile)) {
		name := filepath.Base(path)
		if _, ok := sums[name]; ok && name != snapshotFile {
			continue // sealed, so it's the same file
		}
		sum, n, err := copyFile(path, filepath.Join(to, name))
		if err != nil {
			return b, err
		}
		sums[name] = sum
		b.Bytes += n
		if name != snapshotFile {
			b.New++
		}
	}
	b.Journals = len(journals)
	return b, writeSums(to, sums)
}

// Restore copies a backup into dir, which must be empty, and checks every file against its checksum
// on the way. Then an engine started on dir is where the engine was at the snapshot.
func Restore(from, dir string) (Backed, error) {
	var b Backed
	if names, _ := os.ReadDir(dir); len(names) > 0 {
		return b, fmt.Errorf("%s isn't empty: a restore never mixes two histories", dir)
	}
	sums, err := readSums(from)
	if err != nil {
		return b, err
	}
	if _, ok := sums[snapshotFile]; !ok {
		return b, fmt.Errorf("%s has no snapshot", from)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return b, err
	}
	for _, name := range slices.Sorted(maps.Keys(sums)) {
		sum, n, err := copyFile(filepath.Join(from, name), filepath.Join(dir, name))
		if err != nil {
			return b, err
		}
		if sum != sums[name] {
			return b, fmt.Errorf("%s doesn't match its checksum: the backup is damaged", name)
		}
		b.Bytes += n
	}
	b.Journals = len(sums) - 1 // all but the snapshot
	return b, nil
}

// Checked is what Check replayed, and the state it ended in.
type Checked struct {
	Journals int    // sealed journals replayed
	Inputs   uint64 // inputs replayed, from the first one
	State    uint32 // a checksum of every book: the same books give the same number
}

// Check proves that the sealed journals in dir and its snapshot agree. It replays every input from the
// first one, through the engine's own code, and the books it ends with must be the snapshot's.
func Check(dir string, instruments ...market.Instrument) (Checked, error) {
	var c Checked
	snap, err := readSnapshot(filepath.Join(dir, snapshotFile))
	if err != nil {
		return c, fmt.Errorf("the snapshot: %w", err)
	}
	journals, err := sealed(dir, snap.Input)
	if err != nil {
		return c, err
	}
	e := &Engine{symbols: map[string]*symbol{}}
	for _, in := range instruments {
		e.symbols[in.Symbol] = &symbol{book: book.New(in)}
	}
	for _, path := range journals {
		if err := replayFile(path, e.replay); err != nil {
			return c, fmt.Errorf("%s: %w", filepath.Base(path), err)
		}
	}
	replayed := &snapshot{Input: e.input}
	for _, s := range e.symbols {
		replayed.Books = append(replayed.Books, s.state())
	}
	slices.SortFunc(replayed.Books, func(a, b bookState) int { return cmp.Compare(a.Symbol, b.Symbol) })
	got, err := encode(replayed)
	if err != nil {
		return c, err
	}
	want, err := encode(snap)
	if err != nil {
		return c, err
	}
	if !bytes.Equal(got, want) {
		return c, fmt.Errorf("the journals replay to other books than the snapshot at input %d", snap.Input)
	}
	return Checked{Journals: len(journals), Inputs: e.input, State: crc32.Checksum(got, castagnoli)}, nil
}

// sealed lists the sealed journals in dir, oldest first, up to the one that ends at input. Each one is
// named after its last input, so they must run from the first input to that one with no gap.
func sealed(dir string, input uint64) ([]string, error) {
	paths, err := filepath.Glob(filepath.Join(dir, journalFile+".*"))
	if err != nil {
		return nil, err
	}
	slices.SortFunc(paths, func(a, b string) int { return cmp.Compare(last(a), last(b)) })
	i := slices.IndexFunc(paths, func(p string) bool { return last(p) == input })
	if i < 0 {
		return nil, fmt.Errorf("no sealed journal ends at input %d, where the snapshot is", input)
	}
	return paths[:i+1], nil
}

// last is the last input in a sealed journal, from its name.
func last(path string) uint64 {
	n, _ := strconv.ParseUint(path[strings.LastIndexByte(path, '.')+1:], 10, 64)
	return n
}

// replayFile hands every entry in a sealed journal to replay. A sealed journal ends cleanly.
func replayFile(path string, replay func(journal.Entry) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	r := journal.NewReader(f)
	for {
		entry, err := r.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := replay(entry); err != nil {
			return err
		}
	}
}

// copyFile copies from to to, whole or not at all, and returns its SHA-256 and its size.
func copyFile(from, to string) (string, int64, error) {
	src, err := os.Open(from)
	if err != nil {
		return "", 0, err
	}
	defer src.Close()
	dst, err := os.Create(to + ".tmp")
	if err != nil {
		return "", 0, err
	}
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(dst, h), src)
	if err == nil {
		err = dst.Sync()
	}
	if err = errors.Join(err, dst.Close()); err == nil {
		err = os.Rename(dst.Name(), to)
	}
	return hex.EncodeToString(h.Sum(nil)), n, err
}

// readSums reads a backup's checksums, by file name. A backup that isn't there yet has none.
func readSums(dir string) (map[string]string, error) {
	f, err := os.Open(filepath.Join(dir, sumsFile))
	if errors.Is(err, fs.ErrNotExist) {
		return map[string]string{}, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	sums := map[string]string{}
	for s := bufio.NewScanner(f); s.Scan(); {
		sum, name, ok := strings.Cut(s.Text(), "  ")
		if !ok {
			return nil, fmt.Errorf("%s has a line that isn't a checksum", sumsFile)
		}
		sums[name] = sum
	}
	return sums, nil
}

// writeSums writes a backup's checksums, sorted by name, whole or not at all.
func writeSums(dir string, sums map[string]string) error {
	var buf bytes.Buffer
	for _, name := range slices.Sorted(maps.Keys(sums)) {
		fmt.Fprintf(&buf, "%s  %s\n", sums[name], name)
	}
	path := filepath.Join(dir, sumsFile)
	if err := os.WriteFile(path+".tmp", buf.Bytes(), 0o644); err != nil {
		return err
	}
	if err := os.Rename(path+".tmp", path); err != nil {
		return err
	}
	return syncDir(dir)
}
