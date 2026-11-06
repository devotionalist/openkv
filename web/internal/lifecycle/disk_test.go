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

package lifecycle

import (
	"os"
	"path/filepath"
	"testing"
)

func TestProbeDisk(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "x"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	info := ProbeDisk(dir)
	if !info.Ok {
		t.Fatalf("expected ok disk info")
	}
	if info.TotalBytes == 0 {
		t.Fatalf("total=0")
	}
	if info.DataBytes < 5 {
		t.Fatalf("data_bytes=%d want >=5", info.DataBytes)
	}
	if info.UsedBytes > info.TotalBytes {
		t.Fatalf("used > total")
	}
}

func TestDataDirForStorage(t *testing.T) {
	root := t.TempDir()
	confDir := filepath.Join(root, "conf")
	_ = os.MkdirAll(confDir, 0o755)
	_ = os.WriteFile(filepath.Join(confDir, "storage.conf"), []byte("data_dir=./data/storage-0\n"), 0o644)
	c := Component{Name: "storage-0", Conf: "conf/storage.conf", Role: "storage"}
	got := DataDirFor(root, c)
	want := filepath.Join(root, "data", "storage-0")
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestDataDirForMetaFallback(t *testing.T) {
	root := t.TempDir()
	confDir := filepath.Join(root, "conf")
	_ = os.MkdirAll(confDir, 0o755)
	_ = os.WriteFile(filepath.Join(confDir, "bootstrap.conf"), []byte("node_id=meta-0\n"), 0o644)
	c := Component{Name: "meta-0", Conf: "conf/bootstrap.conf", Role: "meta"}
	got := DataDirFor(root, c)
	want := filepath.Join(root, "data", "meta-0")
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}
