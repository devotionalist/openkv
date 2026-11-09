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
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	cli, err := kv.Dial(ctx, addr)
	if err != nil {
		fatal(err)
	}
	defer cli.Close()
	key := []byte(fmt.Sprintf("ttl-demo-%d", time.Now().UnixNano()))

	missing, err := cli.TTL(ctx, key)
	if err != nil {
		fatal(err)
	}
	if missing != kv.TTLMissing {
		fatal(fmt.Errorf("want TTLMissing got %d", missing))
	}

	if _, err := cli.PutTTL(ctx, key, []byte("ephemeral"), 2000); err != nil {
		fatal(err)
	}
	ttl, err := cli.TTL(ctx, key)
	if err != nil || ttl <= 0 || ttl > 2000 {
		fatal(fmt.Errorf("ttl after put: %d err=%v", ttl, err))
	}
	fmt.Println("ttl remaining=", ttl)

	found, _, err := cli.Expire(ctx, key, 5000)
	if err != nil || !found {
		fatal(fmt.Errorf("expire: found=%v err=%v", found, err))
	}
	ttl, err = cli.TTL(ctx, key)
	if err != nil || ttl < 4000 {
		fatal(fmt.Errorf("ttl after expire: %d err=%v", ttl, err))
	}

	found, _, err = cli.Persist(ctx, key)
	if err != nil || !found {
		fatal(fmt.Errorf("persist: found=%v err=%v", found, err))
	}
	ttl, err = cli.TTL(ctx, key)
	if err != nil || ttl != kv.TTLNoExpire {
		fatal(fmt.Errorf("want permanent got %d err=%v", ttl, err))
	}

	if _, err := cli.PutTTL(ctx, key, []byte("short"), 400); err != nil {
		fatal(err)
	}
	time.Sleep(600 * time.Millisecond)
	_, got, _, err := cli.Get(ctx, key)
	if err != nil {
		fatal(err)
	}
	if got {
		fatal(fmt.Errorf("expected miss after TTL"))
	}
	ttl, err = cli.TTL(ctx, key)
	if err != nil || ttl != kv.TTLMissing {
		fatal(fmt.Errorf("want missing after expire got %d err=%v", ttl, err))
	}
	fmt.Println("ttl example OK")
}
