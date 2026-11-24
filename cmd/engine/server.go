package main

import (
	"context"
	"errors"
	"math"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	boursev1 "github.com/fiston-user/bourse/gen/bourse/v1"
	"github.com/fiston-user/bourse/internal/engine"
	"github.com/fiston-user/bourse/internal/market"
)

// server is the engine's gRPC API. It turns requests into commands, and reports into responses.
type server struct {
	boursev1.UnimplementedEngineServiceServer
	engine *engine.Engine
}

func (s *server) SubmitOrder(ctx context.Context, req *boursev1.SubmitOrderRequest) (*boursev1.SubmitOrderResponse, error) {
	in, ok := s.engine.Instrument(req.GetSymbol())
	if !ok {
		return nil, status.Error(codes.NotFound, engine.ErrUnknownSymbol.Error())
	}
	o := market.Order{
		Symbol:   req.GetSymbol(),
		Account:  req.GetAccount(),
		Side:     narrow[market.Side](req.GetSide()),
		Type:     narrow[market.Type](req.GetType()),
		TIF:      narrow[market.TimeInForce](req.GetTimeInForce()),
		Qty:      market.Qty(req.GetQty()),
		PostOnly: req.GetPostOnly(),
	}
	if p := req.GetPrice(); p != "" {
		price, err := in.ParsePrice(p)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		o.Price = price
	}
	r, err := s.engine.Submit(ctx, o)
	if err != nil {
		return nil, toStatus(err)
	}
	return &boursev1.SubmitOrderResponse{OrderId: r.OrderID, Events: events(in, r.Events)}, nil
}

func (s *server) CancelOrder(ctx context.Context, req *boursev1.CancelOrderRequest) (*boursev1.CancelOrderResponse, error) {
	in, ok := s.engine.Instrument(req.GetSymbol())
	if !ok {
		return nil, status.Error(codes.NotFound, engine.ErrUnknownSymbol.Error())
	}
	r, err := s.engine.Cancel(ctx, req.GetSymbol(), req.GetOrderId())
	if err != nil {
		return nil, toStatus(err)
	}
	return &boursev1.CancelOrderResponse{Events: events(in, r.Events)}, nil
}

// narrow turns a wire enum into ours. A value that doesn't fit becomes zero, and Validate rejects zero.
func narrow[T ~uint8, W ~int32](v W) T {
	if v < 0 || v > math.MaxUint8 {
		return 0
	}
	return T(v)
}

// events turns the engine's events into the wire's, with prices as decimal strings.
func events(in market.Instrument, evs []engine.Event) []*boursev1.Event {
	out := make([]*boursev1.Event, len(evs))
	for i, ev := range evs {
		out[i] = &boursev1.Event{Seq: ev.Seq, Kind: boursev1.EventKind(ev.Kind), OrderId: ev.Order,
			MakerId: ev.Maker, Qty: int64(ev.Qty)}
		if ev.Price != 0 {
			out[i].Price = in.FormatPrice(ev.Price)
		}
		if ev.Err != nil {
			out[i].Reason = ev.Err.Error()
		}
	}
	return out
}

// toStatus turns an engine error into a gRPC status, so a client can tell a bad order from a broken engine.
func toStatus(err error) error {
	switch {
	case errors.Is(err, engine.ErrInvalid):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, engine.ErrUnknownSymbol):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, engine.ErrStopped):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(err).Err()
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
