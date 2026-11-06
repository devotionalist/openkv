# jemalloc 内存分配器

进程级分配器使用 **[jemalloc](https://github.com/jemalloc/jemalloc)**（与 RocksDB / Seastar 一样经 FetchContent 编进工程）。

## 开关

| 选项 | 默认 | 说明 |
|------|------|------|
| `OPSKV_USE_JEMALLOC` | **ON** | `cmake/FetchJemalloc.cmake` 拉取并编译静态库 |
| `OPSKV_JEMALLOC_VERSION` | `5.3.0` | GitHub release tag |
| ASan / TSan | — | 与 jemalloc 冲突；开启 sanitizer 时自动 `USE_JEMALLOC=OFF` |

```bash
cmake -B build -DOPSKV_USE_JEMALLOC=ON   # 默认
cmake -B build -DOPSKV_USE_JEMALLOC=OFF  # 回退系统 malloc
```

## 行为

- 配置：`--with-jemalloc-prefix=`（导出标准 `malloc`/`free`），替换 libc 分配器。
- 链接：`opskv_link_jemalloc(<exe>)` 挂到 `meta_server`、`storage_node`、`compute_node`、`skv`、`opskv_unit_tests`。
- 编译宏：`OPSKV_HAS_JEMALLOC=1`（可选用 `mallctl` 做诊断，非必须）。

RocksDB / 业务 `new`/`malloc` 均走同一进程分配器，无需再开 RocksDB `WITH_JEMALLOC`。
