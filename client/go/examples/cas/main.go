// Copyright 2026 devotionalist
//
package main

import (
	"context"
	"errors"
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

	key := []byte("demo/cas")
	_, _, _ = cli.Delete(ctx, key)

	rev, err := cli.PutCAS(ctx, key, []byte("v1"), 0)
	must(err)
	fmt.Println("create-only ok rev=", rev)

	_, err = cli.PutCAS(ctx, key, []byte("nope"), 0)
	var ce *kv.CasError
	if !errors.As(err, &ce) {
		fatal(fmt.Errorf("expected CasError, got %v", err))
	}
	fmt.Println("create-only conflict current=", ce.Current)

	_, found, cur, err := cli.Get(ctx, key)
	must(err)
	if !found {
		fatal(fmt.Errorf("missing key"))
	}
	rev2, err := cli.PutCAS(ctx, key, []byte("v2"), cur)
	must(err)
	fmt.Println("cas update ok rev=", rev2, " from=", cur)

	_, err = cli.PutCAS(ctx, key, []byte("stale"), cur)
	if !errors.As(err, &ce) {
		fatal(fmt.Errorf("expected stale CasError, got %v", err))
	}
	fmt.Println("stale cas_failed current=", ce.Current)
	fmt.Println("PASS")
}

func must(err error) {
	if err != nil {
		fatal(err)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
