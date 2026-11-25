package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	boursev1 "github.com/fiston-user/bourse/gen/bourse/v1"
	"github.com/fiston-user/bourse/internal/ledger"
	"github.com/fiston-user/bourse/internal/money"
)

// deposit puts cash or shares into an account, from outside the exchange: a wire from a bank, shares
// moved in from a broker. It's an event on the deposits topic, so the ledger and the accounts both
// apply it, each in its own database. Its arguments are an account, then assets and amounts.
func deposit(ctx context.Context, kafka kgo.Opt, args []string) error {
	if len(args) < 3 || len(args)%2 == 0 {
		return errors.New("usage: ledger deposit ACCOUNT ASSET AMOUNT [ASSET AMOUNT]...")
	}
	cl, err := kgo.NewClient(kafka)
	if err != nil {
		return err
	}
	defer cl.Close()
	if err := ledger.EnsureTopics(ctx, cl); err != nil {
		return err
	}
	account := args[0]
	var records []*kgo.Record
	for i := 1; i < len(args); i += 2 {
		asset := args[i]
		amount, err := money.Parse(asset, args[i+1])
		if err != nil || amount <= 0 {
			return fmt.Errorf("%s %s isn't an amount to deposit", asset, args[i+1])
		}
		value, err := proto.Marshal(&boursev1.Deposit{Account: account, Asset: asset, Amount: amount})
		if err != nil {
			return err
		}
		records = append(records, &kgo.Record{Topic: ledger.Deposits, Key: []byte(account), Value: value})
	}
	if err := cl.ProduceSync(ctx, records...).FirstErr(); err != nil {
		return err
	}
	for i := 1; i < len(args); i += 2 {
		fmt.Printf("%s +%s %s\n", account, args[i+1], args[i])
	}
	return nil
}
