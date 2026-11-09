// Copyright 2026 devotionalist
//
// Licensed under the Apache License, Version 2.0 (the "License");
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/devotionalist/openkv/client/go/kv"
)

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "ttl example: %v\n", err)
	os.Exit(1)
}

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
	key := []byte(fmt.Sprintf("ttl-demo-%d", time.Now().UnixNano()))

	if _, err := cli.PutTTL(ctx, key, []byte("ephemeral"), 500); err != nil {
		fatal(err)
	}
	val, found, _, err := cli.Get(ctx, key)
	if err != nil || !found || string(val) != "ephemeral" {
		fatal(fmt.Errorf("expected hit before expire, found=%v val=%q err=%v", found, val, err))
	}
	time.Sleep(700 * time.Millisecond)
	_, found, _, err = cli.Get(ctx, key)
	if err != nil {
		fatal(err)
	}
	if found {
		fatal(fmt.Errorf("expected miss after TTL"))
	}
	fmt.Println("ttl example OK")
}
