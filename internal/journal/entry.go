package journal

import (
	"encoding/binary"
	"errors"

	"github.com/fiston-user/bourse/internal/market"
)

// Entry is one input to the engine: a new order, or a cancel.
type Entry struct {
	Seq    uint64       // counts the engine's inputs, from one, with no gaps
	Time   int64        // when the engine took it, in UTC nanoseconds: a replay reads this, never the clock
	Symbol string       // the book it's for
	Cancel uint64       // the order to cancel, or zero for a new order
	Order  market.Order // the new order, without an ID: the engine hands those out again on replay
}

// The first byte after the time says what kind of entry follows.
const (
	kindOrder  = 1
	kindCancel = 2
)

// AppendBinary encodes e onto the end of b.
func (e *Entry) AppendBinary(b []byte) ([]byte, error) {
	b = binary.LittleEndian.AppendUint64(b, e.Seq)
	b = binary.LittleEndian.AppendUint64(b, uint64(e.Time))
	if e.Cancel != 0 {
		b = append(b, kindCancel)
		b = appendString(b, e.Symbol)
		return binary.LittleEndian.AppendUint64(b, e.Cancel), nil
	}
	o := &e.Order
	var postOnly byte
	if o.PostOnly {
		postOnly = 1
	}
	b = append(b, kindOrder)
	b = appendString(b, e.Symbol)
	b = append(b, byte(o.Side), byte(o.Type), byte(o.TIF), postOnly)
	b = binary.LittleEndian.AppendUint64(b, uint64(o.Price))
	b = binary.LittleEndian.AppendUint64(b, uint64(o.Qty))
	b = appendString(b, o.Account)
	return appendString(b, o.ClientID), nil
}

// appendString encodes s as its length, as a varint, and then its bytes.
func appendString(b []byte, s string) []byte {
	b = binary.AppendUvarint(b, uint64(len(s)))
	return append(b, s...)
}

// errEntry means a payload passed its checksum but isn't an entry we could have written.
var errEntry = errors.New("not a valid entry")

// UnmarshalBinary decodes an entry that AppendBinary encoded.
func (e *Entry) UnmarshalBinary(b []byte) error {
	d := decoder{b: b}
	*e = Entry{Seq: d.u64(), Time: int64(d.u64())}
	kind := d.u8()
	e.Symbol = d.str()
	switch kind {
	case kindCancel:
		e.Cancel = d.u64()
		if e.Cancel == 0 {
			d.bad = true
		}
	case kindOrder:
		o := &e.Order
		o.Symbol = e.Symbol // an order's symbol is the entry's, so it's only written once
		o.Side, o.Type, o.TIF = market.Side(d.u8()), market.Type(d.u8()), market.TimeInForce(d.u8())
		o.PostOnly = d.u8() == 1
		o.Price, o.Qty = market.Price(d.u64()), market.Qty(d.u64())
		o.Account, o.ClientID = d.str(), d.str()
	default:
		d.bad = true
	}
	if d.bad || len(d.b) > 0 {
		return errEntry
	}
	return nil
}

// decoder reads an entry's fields in order. Running out of bytes sets bad, and never panics.
type decoder struct {
	b   []byte
	bad bool
}

func (d *decoder) u8() byte {
	if len(d.b) < 1 {
		d.bad = true
		return 0
	}
	v := d.b[0]
	d.b = d.b[1:]
	return v
}

func (d *decoder) u64() uint64 {
	if len(d.b) < 8 {
		d.bad = true
		return 0
	}
	v := binary.LittleEndian.Uint64(d.b)
	d.b = d.b[8:]
	return v
}

func (d *decoder) str() string {
	n, k := binary.Uvarint(d.b)
	if k <= 0 || n > uint64(len(d.b)-k) {
		d.bad = true
		return ""
	}
	s := string(d.b[k : k+int(n)])
	d.b = d.b[k+int(n):]
	return s
}
