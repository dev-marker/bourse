// Package ledger keeps the exchange's books with double-entry accounting. Every trade and every deposit
// is a transaction of postings, and in each asset, a transaction's postings add up to zero.
package ledger

import (
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	boursev1 "github.com/fiston-user/bourse/gen/bourse/v1"
)

// Accounts that belong to no trader. Bank is the world outside the exchange, so its balance is minus
// everything ever deposited. Fees is what the exchange has earned.
const (
	Bank = "bank"
	Fees = "exchange:fees"
)

// Cash is the asset cash is kept in: US dollars, in cents. Shares are kept under their symbol.
const Cash = "USD"

// Posting moves an amount into an account, or out of it when the amount is negative.
type Posting struct {
	Account string
	Asset   string
	Amount  int64
}

// Fees are in basis points, hundredths of a percent, of what a trade is worth.
const (
	TakerBps = 5 // the order that came in and crossed the book
	MakerBps = 1 // the order that was resting there
)

// Fee is what a trade worth notional cents costs at bps basis points, rounded up to a whole cent.
func Fee(notional, bps int64) int64 {
	return (notional*bps + 9_999) / 10_000
}

// Postings is what an execution moves. Only a trade moves anything: the buyer pays the seller, the
// seller hands over the shares, and each pays a fee to the exchange.
func Postings(x *boursev1.Execution) []Posting {
	if x.GetKind() != boursev1.EventKind_EVENT_KIND_TRADED {
		return nil
	}
	notional := x.GetPrice() * x.GetQty()
	buyer, buyerFee := x.GetAccount(), Fee(notional, TakerBps)
	seller, sellerFee := x.GetMakerAccount(), Fee(notional, MakerBps)
	if x.GetSide() == boursev1.Side_SIDE_SELL {
		buyer, seller = seller, buyer
		buyerFee, sellerFee = sellerFee, buyerFee
	}
	return []Posting{
		{buyer, Cash, -notional - buyerFee},
		{seller, Cash, notional - sellerFee},
		{Fees, Cash, buyerFee + sellerFee},
		{seller, x.GetSymbol(), -x.GetQty()},
		{buyer, x.GetSymbol(), x.GetQty()},
	}
}

// The topics the books are kept from.
const (
	Executions = "executions"
	Deposits   = "deposits"
)

// Event is one record from either topic, with what it moves.
type Event struct {
	Stream    string // the execution's symbol, or deposits
	Seq       uint64 // from one, with no gaps, in each stream
	Time      time.Time
	Execution *boursev1.Execution // nil for a deposit
	Postings  []Posting
}

// Decode reads a record from either topic.
func Decode(r *kgo.Record) (Event, error) {
	if r.Topic == Deposits {
		d := &boursev1.Deposit{}
		if err := proto.Unmarshal(r.Value, d); err != nil {
			return Event{}, err
		}
		// Kafka numbers a partition's records from zero, with no gaps, so the offset numbers a deposit.
		return FromDeposit(uint64(r.Offset)+1, r.Timestamp, d), nil
	}
	x := &boursev1.Execution{}
	if err := proto.Unmarshal(r.Value, x); err != nil {
		return Event{}, err
	}
	return FromExecution(x), nil
}

// FromExecution is an execution, as an event.
func FromExecution(x *boursev1.Execution) Event {
	return Event{Stream: x.GetSymbol(), Seq: x.GetSeq(), Time: time.Unix(0, x.GetTime()).UTC(),
		Execution: x, Postings: Postings(x)}
}

// FromDeposit is the seq-th deposit, as an event. It comes from the bank.
func FromDeposit(seq uint64, at time.Time, d *boursev1.Deposit) Event {
	return Event{Stream: Deposits, Seq: seq, Time: at, Postings: []Posting{
		{Bank, d.GetAsset(), -d.GetAmount()},
		{d.GetAccount(), d.GetAsset(), d.GetAmount()},
	}}
}
