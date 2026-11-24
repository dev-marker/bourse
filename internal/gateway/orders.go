package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	boursev1 "github.com/fiston-user/bourse/gen/bourse/v1"
	"github.com/fiston-user/bourse/internal/market"
)

// orderRequest is the body of POST /v1/orders.
type orderRequest struct {
	ClientOrderID string `json:"client_order_id"`
	Symbol        string `json:"symbol"`
	Side          string `json:"side"`
	Type          string `json:"type"`          // limit, the default, or market
	TimeInForce   string `json:"time_in_force"` // gtc, the default, ioc or fok
	Price         string `json:"price"`         // a decimal string like "101.25", for a limit order
	Qty           int64  `json:"qty"`
	PostOnly      bool   `json:"post_only"`
}

// orderResponse is what happened to an order, event by event.
type orderResponse struct {
	OrderID       uint64  `json:"order_id"`
	ClientOrderID string  `json:"client_order_id,omitempty"`
	Events        []event `json:"events"`
}

type event struct {
	Seq     uint64 `json:"seq"`
	Kind    string `json:"kind"`
	OrderID uint64 `json:"order_id"`
	MakerID uint64 `json:"maker_id,omitempty"`
	Price   string `json:"price,omitempty"`
	Qty     int64  `json:"qty,omitempty"`
	Reason  string `json:"reason,omitempty"`
}

// The words a request may use. Anything else becomes zero, and Validate turns zero away.
var (
	sides = map[string]market.Side{"buy": market.Buy, "sell": market.Sell}
	types = map[string]market.Type{"": market.Limit, "limit": market.Limit, "market": market.Market}
	tifs  = map[string]market.TimeInForce{"": market.GTC, "gtc": market.GTC, "ioc": market.IOC,
		"fok": market.FOK}
)

// engineTimeout is how long we wait for the engine.
const engineTimeout = 5 * time.Second

func (s *Server) placeOrder(w http.ResponseWriter, r *http.Request) {
	var req orderRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		fail(w, http.StatusBadRequest, "the body isn't a valid order: "+err.Error())
		return
	}
	o, err := s.order(account(r), req)
	if err != nil {
		fail(w, http.StatusBadRequest, err.Error())
		return
	}
	p, first := s.replies.claim(o.Account+"/"+o.ClientID, o)
	if !first {
		s.again(w, r, p, o)
		return
	}
	code, body := s.submit(o)
	s.replies.finish(p, code, body, code == http.StatusCreated)
	write(w, code, body)
}

// again answers a retry with the first answer, once there is one. A client order ID is for one order.
func (s *Server) again(w http.ResponseWriter, r *http.Request, p *pending, o market.Order) {
	if p.order != o {
		fail(w, http.StatusConflict, "this client_order_id was already used for a different order")
		return
	}
	select {
	case <-p.done:
	case <-r.Context().Done():
		return
	}
	w.Header().Set("Idempotent-Replay", "true")
	write(w, p.status, p.body)
}

// order turns a request into an order for this account, and checks it exactly the way the engine will.
func (s *Server) order(account string, req orderRequest) (market.Order, error) {
	if req.ClientOrderID == "" {
		return market.Order{}, errors.New("every order needs a client_order_id")
	}
	in, ok := s.instruments[req.Symbol]
	if !ok {
		return market.Order{}, fmt.Errorf("we don't trade %q", req.Symbol)
	}
	o := market.Order{ClientID: req.ClientOrderID, Account: account, Symbol: req.Symbol,
		Side: sides[req.Side], Type: types[req.Type], TIF: tifs[req.TimeInForce], Qty: market.Qty(req.Qty),
		PostOnly: req.PostOnly}
	if req.Price != "" {
		p, err := in.ParsePrice(req.Price)
		if err != nil {
			return market.Order{}, err
		}
		o.Price = p
	}
	return o, o.Validate(in)
}

// submit sends o to the engine, and returns the status and the body to answer with.
func (s *Server) submit(o market.Order) (int, []byte) {
	// Not the request's context. If the client hangs up, the order still goes in, and we see what happened.
	ctx, cancel := context.WithTimeout(context.Background(), engineTimeout)
	defer cancel()
	req := &boursev1.SubmitOrderRequest{Symbol: o.Symbol, Account: o.Account, ClientId: o.ClientID,
		Side: boursev1.Side(o.Side), Type: boursev1.OrderType(o.Type),
		TimeInForce: boursev1.TimeInForce(o.TIF), Qty: int64(o.Qty), PostOnly: o.PostOnly}
	if o.Type == market.Limit {
		req.Price = s.instruments[o.Symbol].FormatPrice(o.Price)
	}
	res, err := s.engine.SubmitOrder(ctx, req)
	if err != nil {
		return s.engineError(err)
	}
	return encode(http.StatusCreated, orderResponse{res.GetOrderId(), o.ClientID, events(res.GetEvents())})
}

// engineError turns an engine's error into a status and a body: the engine's own words for a bad
// request, and a vaguer answer when it's the engine that's in trouble.
func (s *Server) engineError(err error) (int, []byte) {
	switch status.Code(err) {
	case codes.InvalidArgument:
		return encode(http.StatusBadRequest, problem{status.Convert(err).Message()})
	case codes.NotFound:
		return encode(http.StatusNotFound, problem{status.Convert(err).Message()})
	case codes.Unavailable:
		return encode(http.StatusServiceUnavailable, problem{"the engine isn't taking orders right now"})
	case codes.DeadlineExceeded:
		return encode(http.StatusGatewayTimeout, problem{"the engine didn't answer in time"})
	default:
		s.log.Error("the engine failed", "err", err)
		return encode(http.StatusBadGateway, problem{"the engine failed"})
	}
}

// events turns the engine's events into ours.
func events(evs []*boursev1.Event) []event {
	out := make([]event, len(evs))
	for i, ev := range evs {
		kind := strings.ToLower(strings.TrimPrefix(ev.GetKind().String(), "EVENT_KIND_"))
		out[i] = event{Seq: ev.GetSeq(), Kind: kind, OrderID: ev.GetOrderId(), MakerID: ev.GetMakerId(),
			Price: ev.GetPrice(), Qty: ev.GetQty(), Reason: ev.GetReason()}
	}
	return out
}

// cancelOrder takes a resting order out of its book.
func (s *Server) cancelOrder(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseUint(r.PathValue("id"), 10, 64)
	if err != nil {
		fail(w, http.StatusBadRequest, "the order ID must be a number")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), engineTimeout)
	defer cancel()
	req := &boursev1.CancelOrderRequest{Symbol: r.PathValue("symbol"), OrderId: id}
	res, err := s.engine.CancelOrder(ctx, req)
	if err != nil {
		status, body := s.engineError(err)
		write(w, status, body)
		return
	}
	evs := events(res.GetEvents())
	if len(evs) == 1 && evs[0].Kind == "rejected" {
		fail(w, http.StatusNotFound, evs[0].Reason)
		return
	}
	reply(w, http.StatusOK, orderResponse{OrderID: id, Events: evs})
}

// listInstruments says what we trade, with tick sizes as prices.
func (s *Server) listInstruments(w http.ResponseWriter, r *http.Request) {
	type instrument struct {
		Symbol   string `json:"symbol"`
		TickSize string `json:"tick_size"`
		LotSize  int64  `json:"lot_size"`
	}
	out := make([]instrument, len(s.list))
	for i, in := range s.list {
		out[i] = instrument{in.Symbol, in.FormatPrice(in.TickSize), int64(in.LotSize)}
	}
	reply(w, http.StatusOK, out)
}
