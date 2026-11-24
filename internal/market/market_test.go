package market

import (
	"errors"
	"strings"
	"testing"
)

var (
	acme = Instrument{Symbol: "ACME", Decimals: 2, TickSize: 1, LotSize: 1}
	glbx = Instrument{Symbol: "GLBX", Decimals: 2, TickSize: 5, LotSize: 100}
)

func TestParsePrice(t *testing.T) {
	tests := []struct {
		in      Instrument
		s       string
		want    Price
		wantErr error
	}{
		{acme, "101.25", 10125, nil},
		{acme, "101.2", 10120, nil},
		{acme, "101", 10100, nil},
		{acme, "0.1", 10, nil},
		{acme, "101.255", 0, ErrBadPrice},
		{acme, "abc", 0, ErrBadPrice},
		{acme, "-5", 0, ErrBadPrice},
		{glbx, "20.05", 2005, nil},
		{glbx, "20.03", 0, ErrOffTick},
	}
	for _, tt := range tests {
		got, err := tt.in.ParsePrice(tt.s)
		if got != tt.want || !errors.Is(err, tt.wantErr) {
			t.Errorf("%s %q: got %d, %v, want %d, %v", tt.in.Symbol, tt.s, got, err, tt.want, tt.wantErr)
		}
		if err == nil && tt.in.FormatPrice(got) != tt.in.FormatPrice(tt.want) {
			t.Errorf("%s %q: formats as %s", tt.in.Symbol, tt.s, tt.in.FormatPrice(got))
		}
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name string
		o    Order
		want error
	}{
		{"limit buy", Order{Side: Buy, Type: Limit, TIF: GTC, Price: 2005, Qty: 300}, nil},
		{"odd lot", Order{Side: Buy, Type: Limit, TIF: GTC, Price: 2005, Qty: 150}, ErrBadQty},
		{"off tick", Order{Side: Sell, Type: Limit, TIF: GTC, Price: 2003, Qty: 100}, ErrOffTick},
		{"market ioc", Order{Side: Sell, Type: Market, TIF: IOC, Qty: 100}, nil},
		{"market gtc", Order{Side: Sell, Type: Market, TIF: GTC, Qty: 100}, ErrMarketRests},
		{"no side", Order{Type: Limit, TIF: GTC, Price: 2005, Qty: 100}, ErrBadSide},
		{"no time in force", Order{Side: Buy, Type: Limit, Price: 2005, Qty: 100}, ErrBadTIF},
		{"long account", Order{Account: strings.Repeat("a", 65)}, ErrLongName},
	}
	for _, tt := range tests {
		if err := tt.o.Validate(glbx); !errors.Is(err, tt.want) {
			t.Errorf("%s: got %v, want %v", tt.name, err, tt.want)
		}
	}
}
