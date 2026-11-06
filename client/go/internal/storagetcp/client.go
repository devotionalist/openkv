// Copyright 2026 devotionalist
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storagetcp

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	storagev1 "github.com/amydemo/opskv/client/go/gen/storage/v1"
	"github.com/amydemo/opskv/client/go/internal/traffic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type Client struct {
	addr    string
	timeout time.Duration
}

// NotLeaderError 表示需重定向到 Leader。
type NotLeaderError struct {
	Redirect string
	Raw      string
}

func (e *NotLeaderError) Error() string {
	if e.Redirect != "" {
		return fmt.Sprintf("not_leader redirect=%s", e.Redirect)
	}
	return e.Raw
}

func New(addr string) *Client {
	// gRPC Raft 复制下 propose/apply 常需数秒；过短会误杀 Prewrite/Commit。
	return &Client{addr: addr, timeout: 15 * time.Second}
}

func (c *Client) Addr() string { return c.addr }

func adminTokenMD(ctx context.Context) context.Context {
	tok := os.Getenv("SKV_ADMIN_TOKEN")
	if tok == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "x-skv-admin-token", tok)
}

func (c *Client) call(line string) (string, error) {
	traffic.AddToStore(c.addr)
	return c.callGRPC(line)
}

func (c *Client) callGRPC(line string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	ctx = adminTokenMD(ctx)
	conn, err := grpc.NewClient(c.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return "", err
	}
	defer conn.Close()
	cli := storagev1.NewAdminServiceClient(conn)
	resp, err := cli.ExecLine(ctx, &storagev1.ExecLineRequest{Line: line})
	if err != nil {
		return "", err
	}
	return resp.GetBody(), nil
}

func parseRedirect(resp string) string {
	const p = "redirect="
	if i := strings.Index(resp, p); i >= 0 {
		return strings.TrimSpace(resp[i+len(p):])
	}
	return ""
}

func (c *Client) Ping() error {
	resp, err := c.call("PING")
	if err != nil {
		return err
	}
	if !strings.HasPrefix(resp, "OK") {
		return fmt.Errorf("ping: %s", resp)
	}
	return nil
}

// CallRaw 发送一行协议（ADMIN 等）。
func (c *Client) CallRaw(line string) (string, error) {
	return c.call(line)
}

func (c *Client) Put(shard uint32, key, value []byte) (uint64, error) {
	resp, err := c.call(fmt.Sprintf("PUT %d %s %s", shard, hex.EncodeToString(key), hex.EncodeToString(value)))
	if err != nil {
		return 0, err
	}
	parts := strings.Fields(resp)
	if len(parts) >= 1 && parts[0] == "ERR" {
		if strings.Contains(resp, "not_leader") {
			return 0, &NotLeaderError{Redirect: parseRedirect(resp), Raw: resp}
		}
		return 0, fmt.Errorf("put: %s", resp)
	}
	if len(parts) < 3 || parts[0] != "OK" || parts[1] != "PUT" {
		return 0, fmt.Errorf("put: %s", resp)
	}
	return strconv.ParseUint(parts[2], 10, 64)
}

func (c *Client) Get(shard uint32, key []byte) (value []byte, found bool, rev uint64, err error) {
	return c.GetOpt(shard, key, true)
}

// GetOpt linearizable=false 可读任意副本本地引擎（可能脏读；非 MVCC）。
func (c *Client) GetOpt(shard uint32, key []byte, linearizable bool) (value []byte, found bool, rev uint64, err error) {
	lin := 1
	if !linearizable {
		lin = 0
	}
	resp, err := c.call(fmt.Sprintf("GET %d %s %d", shard, hex.EncodeToString(key), lin))
	if err != nil {
		return nil, false, 0, err
	}
	parts := strings.Fields(resp)
	if len(parts) >= 1 && parts[0] == "ERR" {
		if strings.Contains(resp, "not_leader") {
			return nil, false, 0, &NotLeaderError{Redirect: parseRedirect(resp), Raw: resp}
		}
		return nil, false, 0, fmt.Errorf("get: %s", resp)
	}
	if len(parts) < 3 || parts[0] != "OK" || parts[1] != "GET" {
		return nil, false, 0, fmt.Errorf("get: %s", resp)
	}
	rev, err = strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		return nil, false, 0, err
	}
	if len(parts) < 4 || parts[3] == "-" {
		return nil, false, rev, nil
	}
	value, err = hex.DecodeString(parts[3])
	if err != nil {
		return nil, false, 0, err
	}
	return value, true, rev, nil
}

func (c *Client) Delete(shard uint32, key []byte) (uint64, error) {
	resp, err := c.call(fmt.Sprintf("DEL %d %s", shard, hex.EncodeToString(key)))
	if err != nil {
		return 0, err
	}
	parts := strings.Fields(resp)
	if len(parts) >= 1 && parts[0] == "ERR" {
		if strings.Contains(resp, "not_leader") {
			return 0, &NotLeaderError{Redirect: parseRedirect(resp), Raw: resp}
		}
		return 0, fmt.Errorf("del: %s", resp)
	}
	if len(parts) < 3 || parts[0] != "OK" || parts[1] != "DEL" {
		return 0, fmt.Errorf("del: %s", resp)
	}
	return strconv.ParseUint(parts[2], 10, 64)
}

func (c *Client) txnErr(resp, op string) error {
	parts := strings.Fields(resp)
	if len(parts) >= 1 && parts[0] == "ERR" {
		if strings.Contains(resp, "not_leader") {
			return &NotLeaderError{Redirect: parseRedirect(resp), Raw: resp}
		}
		return fmt.Errorf("%s: %s", op, resp)
	}
	return fmt.Errorf("%s: %s", op, resp)
}

// LockedError 携带阻塞锁的 primary / lock_ts，供 CheckTxnStatus + Resolve。
type LockedError struct {
	Primary []byte
	LockTs  uint64
	Raw     string
}

func (e *LockedError) Error() string { return e.Raw }

func (c *Client) Prewrite(shard uint32, key []byte, startTs uint64, primary []byte, op uint8, value []byte, epoch uint64) (uint64, error) {
	// 空 value 用 "-" 占位，避免 split_ws 吃掉空字段导致 epoch/value 错位（Delete prewrite）。
	valHex := hex.EncodeToString(value)
	if valHex == "" {
		valHex = "-"
	}
	line := fmt.Sprintf("PREWRITE %d %s %d %s %d %s",
		shard, hex.EncodeToString(key), startTs, hex.EncodeToString(primary), op, valHex)
	if epoch != 0 {
		line += fmt.Sprintf(" %d", epoch)
	}
	resp, err := c.call(line)
	if err != nil {
		return 0, err
	}
	parts := strings.Fields(resp)
	if len(parts) < 3 || parts[0] != "OK" || parts[1] != "PREWRITE" {
		return 0, c.txnErr(resp, "prewrite")
	}
	return strconv.ParseUint(parts[2], 10, 64)
}

func (c *Client) CommitKey(shard uint32, key []byte, startTs, commitTs, epoch uint64) (uint64, error) {
	line := fmt.Sprintf("COMMIT %d %s %d %d", shard, hex.EncodeToString(key), startTs, commitTs)
	if epoch != 0 {
		line += fmt.Sprintf(" %d", epoch)
	}
	resp, err := c.call(line)
	if err != nil {
		return 0, err
	}
	parts := strings.Fields(resp)
	if len(parts) < 3 || parts[0] != "OK" || parts[1] != "COMMIT" {
		return 0, c.txnErr(resp, "commit")
	}
	return strconv.ParseUint(parts[2], 10, 64)
}

func (c *Client) RollbackKey(shard uint32, key []byte, startTs, epoch uint64) (uint64, error) {
	line := fmt.Sprintf("ROLLBACK %d %s %d", shard, hex.EncodeToString(key), startTs)
	if epoch != 0 {
		line += fmt.Sprintf(" %d", epoch)
	}
	resp, err := c.call(line)
	if err != nil {
		return 0, err
	}
	parts := strings.Fields(resp)
	if len(parts) < 3 || parts[0] != "OK" || parts[1] != "ROLLBACK" {
		return 0, c.txnErr(resp, "rollback")
	}
	return strconv.ParseUint(parts[2], 10, 64)
}

func (c *Client) PessimisticLock(shard uint32, key []byte, startTs, forUpdateTs uint64, primary []byte, ttlMs, epoch uint64) (uint64, error) {
	line := fmt.Sprintf("PESSIMISTIC_LOCK %d %s %d %d %s %d",
		shard, hex.EncodeToString(key), startTs, forUpdateTs, hex.EncodeToString(primary), ttlMs)
	if epoch != 0 {
		line += fmt.Sprintf(" %d", epoch)
	}
	resp, err := c.call(line)
	if err != nil {
		return 0, err
	}
	parts := strings.Fields(resp)
	if len(parts) < 3 || parts[0] != "OK" || parts[1] != "PESSIMISTIC_LOCK" {
		return 0, c.txnErr(resp, "pessimistic_lock")
	}
	return strconv.ParseUint(parts[2], 10, 64)
}

func (c *Client) PessimisticRollback(shard uint32, key []byte, startTs, epoch uint64) (uint64, error) {
	line := fmt.Sprintf("PESSIMISTIC_ROLLBACK %d %s %d", shard, hex.EncodeToString(key), startTs)
	if epoch != 0 {
		line += fmt.Sprintf(" %d", epoch)
	}
	resp, err := c.call(line)
	if err != nil {
		return 0, err
	}
	parts := strings.Fields(resp)
	if len(parts) < 3 || parts[0] != "OK" || parts[1] != "PESSIMISTIC_ROLLBACK" {
		return 0, c.txnErr(resp, "pessimistic_rollback")
	}
	return strconv.ParseUint(parts[2], 10, 64)
}

func (c *Client) ResolveLock(shard uint32, key []byte, startTs, commitTs, epoch uint64) (uint64, error) {
	line := fmt.Sprintf("RESOLVE_LOCK %d %s %d %d", shard, hex.EncodeToString(key), startTs, commitTs)
	if epoch != 0 {
		line += fmt.Sprintf(" %d", epoch)
	}
	resp, err := c.call(line)
	if err != nil {
		return 0, err
	}
	parts := strings.Fields(resp)
	if len(parts) < 3 || parts[0] != "OK" || parts[1] != "RESOLVE_LOCK" {
		return 0, c.txnErr(resp, "resolve_lock")
	}
	return strconv.ParseUint(parts[2], 10, 64)
}

func (c *Client) CheckTxnStatus(shard uint32, primary []byte, lockTs uint64) (status string, commitTs uint64, err error) {
	resp, err := c.call(fmt.Sprintf("CHECK_TXN_STATUS %d %s %d", shard, hex.EncodeToString(primary), lockTs))
	if err != nil {
		return "", 0, err
	}
	parts := strings.Fields(resp)
	if len(parts) < 4 || parts[0] != "OK" || parts[1] != "CHECK_TXN_STATUS" {
		return "", 0, c.txnErr(resp, "check_txn_status")
	}
	commitTs, _ = strconv.ParseUint(parts[3], 10, 64)
	return parts[2], commitTs, nil
}

func (c *Client) TxnGet(shard uint32, key []byte, startTs uint64) (value []byte, found bool, err error) {
	return c.TxnGetOpt(shard, key, startTs, true)
}

// TxnGetOpt linearizable=false 可读任意副本本地 MVCC（可能落后）。
func (c *Client) TxnGetOpt(shard uint32, key []byte, startTs uint64, linearizable bool) (value []byte, found bool, err error) {
	lin := 1
	if !linearizable {
		lin = 0
	}
	resp, err := c.call(fmt.Sprintf("TXN_GET %d %s %d %d", shard, hex.EncodeToString(key), startTs, lin))
	if err != nil {
		return nil, false, err
	}
	parts := strings.Fields(resp)
	if len(parts) >= 1 && parts[0] == "ERR" {
		if strings.Contains(resp, "locked") {
			le := &LockedError{Raw: resp}
			for _, f := range parts {
				if strings.HasPrefix(f, "primary=") {
					le.Primary, _ = hex.DecodeString(strings.TrimPrefix(f, "primary="))
				}
				if strings.HasPrefix(f, "lock_ts=") {
					le.LockTs, _ = strconv.ParseUint(strings.TrimPrefix(f, "lock_ts="), 10, 64)
				}
			}
			return nil, false, le
		}
		return nil, false, c.txnErr(resp, "txn_get")
	}
	if len(parts) < 3 || parts[0] != "OK" || parts[1] != "TXN_GET" {
		return nil, false, fmt.Errorf("txn_get: %s", resp)
	}
	if parts[2] == "-" {
		return nil, false, nil
	}
	value, err = hex.DecodeString(parts[2])
	if err != nil {
		return nil, false, err
	}
	return value, true, nil
}

// WatchEvent 与 Storage WATCH_POLL 对齐。
type WatchEvent struct {
	Key      []byte
	Value    []byte
	Revision uint64
	Deleted  bool
}

// WatchHead 返回 region 当前 last_applied，用作 StartRevision=0 的起点。
func (c *Client) WatchHead(shard uint32) (uint64, error) {
	resp, err := c.call(fmt.Sprintf("WATCH_HEAD %d", shard))
	if err != nil {
		return 0, err
	}
	parts := strings.Fields(resp)
	if len(parts) != 3 || parts[0] != "OK" || parts[1] != "WATCH_HEAD" {
		return 0, fmt.Errorf("watch_head: %s", resp)
	}
	return strconv.ParseUint(parts[2], 10, 64)
}

// WatchPoll 拉取 region 上 since 之后的变更；返回事件与新的 since（max_rev）。
func (c *Client) WatchPoll(shard uint32, since uint64, limit int) (events []WatchEvent, nextSince uint64, err error) {
	if limit <= 0 {
		limit = 64
	}
	resp, err := c.call(fmt.Sprintf("WATCH_POLL %d %d %d", shard, since, limit))
	if err != nil {
		return nil, since, err
	}
	parts := strings.Fields(resp)
	if len(parts) < 3 || parts[0] != "OK" || parts[1] != "WATCH" {
		return nil, since, fmt.Errorf("watch_poll: %s", resp)
	}
	n, _ := strconv.Atoi(parts[2])
	nextSince = since
	// OK WATCH n [events...] max_rev
	if len(parts) < 4 {
		return nil, since, nil
	}
	maxRev, err := strconv.ParseUint(parts[len(parts)-1], 10, 64)
	if err != nil {
		return nil, since, fmt.Errorf("watch_poll: %s", resp)
	}
	nextSince = maxRev
	evParts := parts[3 : len(parts)-1]
	for i := 0; i < n && i < len(evParts); i++ {
		tok := evParts[i]
		cols := strings.SplitN(tok, ":", 4)
		if len(cols) < 4 {
			continue
		}
		key, _ := hex.DecodeString(cols[0])
		rev, _ := strconv.ParseUint(cols[1], 10, 64)
		deleted := cols[2] == "D"
		var val []byte
		if !deleted && cols[3] != "-" {
			val, _ = hex.DecodeString(cols[3])
		}
		events = append(events, WatchEvent{Key: key, Value: val, Revision: rev, Deleted: deleted})
	}
	return events, nextSince, nil
}
