// Copyright 2026 devotionalist
//
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/devotionalist/openkv-client-go/kv"
)

func main() {
	addr := "127.0.0.1:19000"
	if len(os.Args) > 1 {
		addr = os.Args[1]
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cli, err := kv.Dial(ctx, addr)
	if err != nil {
		fatal(err)
	}
	defer cli.Close()

	prefix := []byte("demo/scan/")
	_, _, _ = cli.Delete(ctx, append(prefix, []byte("a")...))
	_, _, _ = cli.Delete(ctx, append(prefix, []byte("b")...))
	_, _, _ = cli.Delete(ctx, append(prefix, []byte("c")...))

	must(put(cli, ctx, append(prefix, []byte("a")...), []byte("1")))
	must(put(cli, ctx, append(prefix, []byte("b")...), []byte("2")))
	must(put(cli, ctx, append(prefix, []byte("c")...), []byte("3")))

	res, err := cli.ListPrefix(ctx, prefix, 2)
	must(err)
	fmt.Printf("page1 n=%d truncated=%v next=%q\n", len(res.Pairs), res.Truncated, res.NextKey)
	for _, p := range res.Pairs {
		fmt.Printf("  %s=%s rev=%d\n", p.Key, p.Value, p.Revision)
	}
	if !res.Truncated {
		fatal(fmt.Errorf("expected truncated with limit=2"))
	}
	res2, err := cli.ListPrefix(ctx, prefix, 10)
	must(err)
	// 全量应 3 条
	if len(res2.Pairs) != 3 {
		fatal(fmt.Errorf("want 3 got %d", len(res2.Pairs)))
	}
	fmt.Printf("full n=%d truncated=%v\n", len(res2.Pairs), res2.Truncated)
	fmt.Println("scan smoke PASS")
}

func put(cli *kv.Client, ctx context.Context, k, v []byte) error {
	_, err := cli.Put(ctx, k, v)
	return err
}

func must(err error) {
	if err != nil {
		fatal(err)
	}
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "error: %v\n", err)
	os.Exit(1)
}
