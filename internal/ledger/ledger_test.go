package ledger_test

import (
	"math/rand/v2"
	"testing"

	boursev1 "github.com/fiston-user/bourse/gen/bourse/v1"
	"github.com/fiston-user/bourse/internal/ledger"
)

// TestFee checks the rounding: a fee is rounded up to a whole cent, in the exchange's favour, and an
// exact one isn't rounded at all.
func TestFee(t *testing.T) {
	tests := []struct {
		notional, bps, want int64
	}{
		{100 * 101_25, ledger.TakerBps, 507}, // 5.0625 is 5.07
		{100 * 101_25, ledger.MakerBps, 102}, // 1.0125 is 1.02
		{2_000_00, ledger.TakerBps, 100},     // exactly 1.00
		{1, ledger.TakerBps, 1},              // a fraction of a cent is a cent
		{0, ledger.TakerBps, 0},
	}
	for _, tt := range tests {
		if got := ledger.Fee(tt.notional, tt.bps); got != tt.want {
			t.Errorf("Fee(%d, %d) = %d, want %d", tt.notional, tt.bps, got, tt.want)
		}
	}
}

// TestPostings books ten thousand random trades. Each one must add up to zero in every asset, the buyer
// pays at least what the shares are worth, the seller gets at most that, and the fees make up the rest.
func TestPostings(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	for range 10_000 {
		x := &boursev1.Execution{Symbol: "ACME", Kind: boursev1.EventKind_EVENT_KIND_TRADED,
			Account: "taker", MakerAccount: "maker", Side: boursev1.Side(1 + r.IntN(2)),
			Price: 1 + r.Int64N(1_000_00), Qty: 1 + r.Int64N(10_000)}
		sum := map[string]int64{}
		by := map[string]int64{} // by account and asset
		for _, p := range ledger.Postings(x) {
			sum[p.Asset] += p.Amount
			by[p.Account+" "+p.Asset] += p.Amount
		}
		for asset, n := range sum {
			if n != 0 {
				t.Fatalf("%v: %s adds up to %d", x, asset, n)
			}
		}
		buyer, seller := "taker", "maker"
		if x.GetSide() == boursev1.Side_SIDE_SELL {
			buyer, seller = seller, buyer
		}
		notional, fees := x.GetPrice()*x.GetQty(), by[ledger.Fees+" USD"]
		if -by[buyer+" USD"] < notional || by[seller+" USD"] > notional || fees <= 0 ||
			by[buyer+" ACME"] != x.GetQty() || by[seller+" ACME"] != -x.GetQty() {
			t.Fatalf("%v: the postings are %v", x, by)
		}
	}
}
