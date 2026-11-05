# EdgeCore ServiceAccount Token 缓存轮转修复

## 概述

本文记录 EdgeCore MetaManager 中 ServiceAccount Token 缓存问题的修复计划、实现结果和验证情况。方案参考 [kubeedge/kubeedge#6828](https://github.com/kubeedge/kubeedge/pull/6828)，采用“多代 Token 共存、读取最新有效代、过期后回收”的核心思路，并避免原方案中基于 SQLite `LIKE` 的前缀查询。

修复完成后，Kubernetes 轮转 projected ServiceAccount Token 时，旧 Token 在自身过期前仍保留于 MetaDB。Pod 尚未重新读取新 Token 的传播窗口内，MetaServer 可以同时认证新旧两代 Token，不再因缓存被提前覆盖而返回 `Unauthorized`。

## 背景与根因

Kubelet 会在 ServiceAccount Token 生命周期达到约 80% 时请求刷新。此时旧 Token 尚未过期，Pod 也可能仍从 projected volume 中读取到旧 Token。

修复前，EdgeCore 使用 TokenRequest 的 ServiceAccount、namespace、audiences、TTL 和 BoundObjectRef 生成唯一缓存键。刷新流程存在两个问题：

1. `getTokenLocally` 发现 Token 进入刷新窗口后，立即删除旧缓存。
2. 新 Token 与旧 Token 使用相同缓存键，远端响应落库时覆盖旧记录。

MetaServer 未配置 Token 公钥时，会通过 `CheckTokenExist` 检查 Token 是否存在于 MetaDB。旧 Token 虽然仍满足 JWT 有效期，但其缓存记录已经被删除或覆盖，因此认证失败。

## 修复目标

- 同一逻辑 Token 的不同轮转代可以同时存在。
- 本地读取始终返回过期时间最晚的未过期 Token。
- 进入刷新窗口只触发远端刷新，不提前删除旧 Token。
- 旧 Token 到期后自动回收，避免缓存无限增长。
- 兼容升级前按逻辑键存储的旧缓存记录。
- 缓存键不包含 Token 明文。
- 单条损坏记录不影响其他合法 Token 的认证。
- 不修改数据库表结构和 JWT claims 校验逻辑。

## 修复计划与结果

| 项目 | 计划 | 结果 |
| --- | --- | --- |
| 1 | 拆分逻辑键与代际存储键 | 已完成。新增稳定 `baseKey`；带过期时间的响应使用代际键。 |
| 2 | 修改本地读取逻辑 | 已完成。过滤损坏或过期记录，返回最新未过期代；刷新时不删除旧代。 |
| 3 | 增加索引友好的范围查询 | 已完成。使用主键等值查询加半开区间查询，未使用 `LIKE`。 |
| 4 | 增加过期 Token GC | 已完成。启动时立即清理，之后每分钟清理，随 MetaManager 停止。 |
| 5 | 加固认证扫描 | 已完成。损坏记录被跳过；DB 错误和空结果仍然 fail closed。 |
| 6 | 补充集成和生命周期测试 | 已完成。覆盖真实响应落库链路和完整 Token 生命周期。 |

## 实现详情

### 缓存键

`baseKey` 继续由以下 TokenRequest 字段生成：

- ServiceAccount 名称
- namespace
- audiences
- expirationSeconds
- BoundObjectRef

请求对象通常没有 `Status.ExpirationTimestamp`，此时 `KeyFunc` 返回原有 `baseKey`。远端 TokenResponse 带有过期时间，存储键变为：

```text
<baseKey>/<expirationTimestamp.UnixNano()>
```

相同响应重放会写入同一个键，保持幂等；不同过期时间的 TokenResponse 写入不同键。Token 明文不会进入缓存键或日志。

### 本地读取

`getTokenLocally` 按 `baseKey` 查询 legacy exact-key 记录和所有代际记录，然后：

1. 跳过不属于当前逻辑键的记录。
2. 跳过无法反序列化的记录。
3. 跳过零过期时间或已经过期的记录。
4. 选择 `ExpirationTimestamp` 最大的 Token。
5. 若最新 Token 已进入刷新窗口，返回刷新信号，但保留其数据库记录。

因此，刷新成功后新旧 Token 可以共存；刷新失败时，上层 kubelet TokenManager 仍可继续使用未过期旧 Token。

### SQLite 范围查询

新增 `QueryAllMetaByKeyPrefix`。查询条件等价于：

```sql
key = baseKey
OR (key >= baseKey || '/' AND key < baseKey || '0')
```

SQLite 默认 BINARY collation 下，`/` 紧邻并小于 `0`。该半开区间仅覆盖 `baseKey/` 开头的代际键，同时保留 exact `baseKey` 兼容旧记录。

该查询使用 `meta.key` 主键索引，避免 `LIKE prefix%` 带来的问题：

- 默认大小写不敏感导致误匹配。
- `%` 和 `_` 被解释为通配符。
- SQLite 无法稳定使用默认主键索引，可能产生全表扫描。

### 过期回收

MetaManager 启动时创建 ServiceAccount Token GC goroutine：

- 启动后立即执行一次清理。
- 默认每分钟执行一次。
- 仅删除 `ExpirationTimestamp <= now` 的记录。
- 未过期和零过期时间记录保留。
- 无法解析的记录保留并记录错误，避免静默删除诊断数据。
- 单条删除失败不会阻断后续记录处理。
- `beehiveContext.Done()` 关闭后停止 goroutine。

Pod 删除时，原有 `DeleteServiceAccountToken` 会按 BoundObjectRef UID 删除该 Pod 对应的所有 Token 代。

### 认证扫描

`CheckTokenExist` 仍按 `ResourceTypeServiceAccountToken` 扫描 Token 值，但行为调整为：

- 空 Token、DB 错误和 nil 查询结果返回 false。
- 单条 JSON 损坏时记录错误并继续扫描。
- 只有完整记录中的 Token 与请求 Token 精确相等时才返回 true。

缓存存在性检查不会替代 JWT 校验。MetaServer 后续仍通过 `validator.Validate` 检查 JWT 有效期、ServiceAccount、Pod、Secret 和 UID 等 claims。

## Token 生命周期

修复后的轮转过程：

1. T1 存入 `baseKey/exp1`。
2. T1 达到刷新阈值，但尚未过期；EdgeCore 请求 T2，不删除 T1。
3. T2 存入 `baseKey/exp2`。
4. 传播窗口内，T1 和 T2 均存在；MetaServer 可认证两者，本地读取返回 T2。
5. T1 到期后，GC 删除 `baseKey/exp1`。
6. T2 保留并继续使用，直到下一次轮转或 Pod 删除。

在常规 80% TTL 刷新策略下，每个逻辑 Token 通常只保留一到两代。

## 代码变更

| 文件 | 内容 |
| --- | --- |
| `edge/pkg/metamanager/client/serviceaccount.go` | 多代键、本地选择、认证扫描、过期 GC。 |
| `edge/pkg/metamanager/dao/dbclient/meta.go` | 主键范围查询。 |
| `edge/pkg/metamanager/metamanager.go` | GC 生命周期接入。 |
| `edge/pkg/metamanager/client/serviceaccount_test.go` | 键、读取、认证、GC、Pod 删除和完整生命周期测试。 |
| `edge/pkg/metamanager/dao/dbclient/meta_test.go` | 范围、大小写、通配符隔离和查询计划测试。 |
| `edge/pkg/metamanager/process_test.go` | 真实 TokenResponse 落库和多代共存测试。 |

`edge/pkg/metamanager/process.go` 无需修改；现有 `parseResource` 已调用 `client.KeyFunc`，因此 TokenResponse 会自动使用代际键。

## 测试与验证

执行的相关命令：

```bash
go test ./edge/pkg/metamanager -count=1
go test ./edge/pkg/metamanager/client \
  -run '^(TestServiceAccountToken.*|TestNewestUnexpiredToken.*|TestTokenExists.*|TestGCExpiredTokensOnce.*|TestRunExpiredTokenGCStops)$' \
  -count=1
go test ./edge/pkg/metamanager/dao/dbclient \
  -run '^TestQueryAllMetaByKeyPrefix' \
  -count=1
go vet ./edge/pkg/metamanager/client \
  ./edge/pkg/metamanager/dao/dbclient \
  ./edge/pkg/metamanager
```

验证结果：

- MetaManager 包：3 个测试通过。
- ServiceAccount Token 相关测试：14 个测试通过。
- MetaDB 范围查询测试：2 个测试通过。
- 关键集成测试使用 `-count=2` 重复运行通过。
- `go vet` 未发现问题。

测试覆盖以下关键场景：

- legacy exact-key 兼容。
- 不同 Token 代生成不同键。
- 同一响应重放幂等。
- 真实 `ResponseOperation -> parseResource -> KeyFunc -> InsertOrUpdate` 落库链路。
- 新旧 Token 重叠期同时认证。
- 本地读取返回最新未过期 Token。
- 刷新候选不会被提前删除。
- GC 仅删除过期代，新代继续认证。
- Pod 删除清理全部 Token 代。
- 损坏记录不阻断后续合法 Token。
- `%`、`_`、大小写和普通字符串前缀不会误匹配。
- SQLite 查询计划不包含全表扫描。

## 兼容性与限制

- 无数据库 schema 变更；升级时可继续读取旧 exact-key 记录。
- 降级到旧版 EdgeCore 后，旧实现无法离线读取新的代际键；云端连接正常时可重新获取 Token。
- 代际唯一性使用 `ExpirationTimestamp.UnixNano()`。相同响应重放会覆盖同一行，这是预期幂等行为。
- `CheckTokenExist` 仍是按 Token 类型扫描并反序列化记录；认证索引优化不属于本次修复范围。
- GC 每分钟按 Token 类型扫描一次。其频率远低于认证请求，且多代记录数量受过期回收限制。
- 本地 Go 1.25.12 环境下，完整 client 测试包中的既有 `gomonkey` 用例 `TestPersistentVolumeClaimsGet/SendSync_Error` 会触发 SIGBUS。该问题与本修复无关；项目 `go.mod` 声明 Go 1.23.12，本次相关测试均已独立通过。

## 结论

六项修复均已完成。EdgeCore 现在可以在 ServiceAccount Token 轮转传播窗口内保留并认证新旧 Token，同时通过索引查询和周期 GC 控制读取成本与缓存规模。
