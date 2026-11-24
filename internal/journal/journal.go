// Package journal is a write-ahead log. Every input to the engine is written here, and synced to
// disk, before any book sees it.
//
// A journal is a file of records, one after another. A record is an 8-byte header, the payload's
// length and its CRC-32C checksum, both little-endian, and then the payload, one encoded Entry.
package journal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

const (
	header    = 8       // a record's length and checksum
	maxRecord = 1 << 16 // the largest payload, so a damaged length can't ask for gigabytes
)

// castagnoli is the table for CRC-32C, the checksum modern CPUs compute in hardware.
var castagnoli = crc32.MakeTable(crc32.Castagnoli)

var (
	ErrTorn    = errors.New("the journal ends in the middle of a record")
	ErrCorrupt = errors.New("a record in the journal is damaged")
)

// Writer appends entries to a journal. It isn't safe for concurrent use: a journal has one writer.
type Writer struct {
	f       *os.File
	buf     []byte // records appended since the last Sync
	scratch []byte // where an entry is encoded
}

// Create starts a new journal at path. It refuses a file that already exists, so two histories never mix.
func Create(path string) (*Writer, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	// A new file only survives a power cut once its folder's entry for it is on disk too.
	if err := syncDir(filepath.Dir(path)); err != nil {
		f.Close()
		return nil, err
	}
	return &Writer{f: f}, nil
}

// syncDir waits until the disk has dir's list of files.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// Append adds e to the journal. It's only durable after the next Sync.
func (w *Writer) Append(e *Entry) error {
	payload, _ := e.AppendBinary(w.scratch[:0])
	if len(payload) > maxRecord {
		return fmt.Errorf("an entry of %d bytes is too big for the journal", len(payload))
	}
	w.scratch = payload
	w.buf = binary.LittleEndian.AppendUint32(w.buf, uint32(len(payload)))
	w.buf = binary.LittleEndian.AppendUint32(w.buf, crc32.Checksum(payload, castagnoli))
	w.buf = append(w.buf, payload...)
	return nil
}

// Sync writes every appended entry to the file, and waits until the disk has them.
func (w *Writer) Sync() error {
	if _, err := w.f.Write(w.buf); err != nil {
		return err
	}
	w.buf = w.buf[:0]
	return w.f.Sync()
}

// Close closes the file. Entries appended since the last Sync are lost.
func (w *Writer) Close() error {
	return w.f.Close()
}

// Reader reads entries back from a journal, and checks every record on the way.
type Reader struct {
	r   *bufio.Reader
	off int64 // where the next record starts
}

// NewReader reads a journal from r.
func NewReader(r io.Reader) *Reader {
	return &Reader{r: bufio.NewReader(r)}
}

// Next returns the next entry, or io.EOF at the clean end of the journal. A journal that ends in the
// middle of a record gives ErrTorn, and a record that fails its checks gives ErrCorrupt.
func (r *Reader) Next() (Entry, error) {
	var h [header]byte
	if _, err := io.ReadFull(r.r, h[:]); err == io.EOF {
		return Entry{}, io.EOF
	} else if err != nil {
		return Entry{}, r.damaged(err)
	}
	size, sum := binary.LittleEndian.Uint32(h[:4]), binary.LittleEndian.Uint32(h[4:])
	if size == 0 || size > maxRecord {
		return Entry{}, r.damaged(ErrCorrupt)
	}
	payload := make([]byte, size)
	if _, err := io.ReadFull(r.r, payload); err != nil {
		return Entry{}, r.damaged(err)
	}
	if crc32.Checksum(payload, castagnoli) != sum {
		return Entry{}, r.damaged(ErrCorrupt)
	}
	var e Entry
	if err := e.UnmarshalBinary(payload); err != nil {
		return Entry{}, r.damaged(ErrCorrupt)
	}
	r.off += header + int64(size)
	return e, nil
}

// damaged says what's wrong, and where the bad record starts. A record that's cut short is torn.
func (r *Reader) damaged(err error) error {
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		err = ErrTorn
	}
	return fmt.Errorf("%w, at offset %d", err, r.off)
}

// Offset is where the next record starts. After an error, it's the end of the last good record.
func (r *Reader) Offset() int64 {
	return r.off
}
