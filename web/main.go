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

// OpsKV Web 管控模块：独立进程，默认 :19100。
//
//	cd web && go run . -listen 0.0.0.0:19100
package main

import (
	"embed"
	"flag"
	"io/fs"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/amydemo/opskv/web/internal/api"
)

//go:embed static/*
var staticFS embed.FS

func main() {
	listen := flag.String("listen", "0.0.0.0:19100", "HTTP listen")
	metas := flag.String("metas", envOr("SKV_META", "127.0.0.1:8500,127.0.0.1:8501,127.0.0.1:8502"), "meta TCP")
	storages := flag.String("storages", envOr("SKV_STORAGES", "127.0.0.1:9100,127.0.0.1:9101,127.0.0.1:9102"), "storage TCP")
	gateway := flag.String("gateway", envOr("SKV_GATEWAY", "127.0.0.1:19000"), "gateway gRPC")
	runDir := flag.String("run-dir", envOr("SKV_RUN", ""), ".run dir for process panel / 节点上下线")
	root := flag.String("root", envOr("SKV_ROOT", ""), "repo root（启停二进制用）")
	flag.Parse()

	svc := &api.Server{
		MetaAddrs: splitCSV(*metas),
		Storages:  splitCSV(*storages),
		Gateway:   *gateway,
		RunDir:    *runDir,
		Root:      *root,
	}

	mux := http.NewServeMux()
	mux.Handle("/api/", svc.Handler())

	sub, err := fs.Sub(staticFS, "static")
	if err != nil {
		log.Fatal(err)
	}
	mux.Handle("/", http.FileServer(http.FS(sub)))

	addr := *listen
	display := addr
	if strings.HasPrefix(addr, "0.0.0.0:") {
		display = "127.0.0.1:" + strings.TrimPrefix(addr, "0.0.0.0:")
	}
	log.Printf("OpsKV web module → http://%s", display)

	srv := &http.Server{
		Addr:              addr,
		Handler:           withCORS(mux),
		ReadHeaderTimeout: 5 * time.Second,
	}
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(204)
			return
		}
		next.ServeHTTP(w, r)
	})
}
