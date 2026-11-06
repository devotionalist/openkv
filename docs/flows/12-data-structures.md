# 12 — 数据结构读写（DSService）

> 设计总览：[DATA_STRUCTURES.md](../architecture/DATA_STRUCTURES.md)

## 写路径（例：HSet）

```mermaid
flowchart TD
  c[Client_HSet] --> gw[Gateway_DSService]
  gw --> mk["MetaKey = ukey + nul + ds"]
  gw --> loc[Locate_MetaKey]
  gw --> ts[Meta_AllocTs]
  gw --> get[TxnGet_MetaKey]
  get -->|wrong_type| err[FailedPrecondition]
  get --> mut[Decode_blob_mutate_Encode]
  mut --> pw[TxnPrewrite]
  pw --> cm[TxnCommit]
  cm --> ok[HSetResponse]
```

## 读路径（例：HGetAll）

```mermaid
flowchart TD
  c[Client_HGetAll] --> gw[Gateway]
  gw --> mk[MetaKey]
  gw --> get[TxnGet_or_Get]
  get -->|miss| empty[empty_map]
  get -->|ok| dec[Decode_Hash]
  dec --> resp[fields_and_values]
```

## 与普通 KV

- 普通 `Put(ukey)` **不会**覆盖 `MetaKey(ukey,'h')`
- DS 对象删除至空时删除 MetaKey（Del 短事务）
