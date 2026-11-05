# EdgeCore 重启时过早清理存量 Pod 卷

## 现场证据（2026-09-23）

以下是用户提供的现场日志和只读采集结果，不是本机部署验证。

对同一个 Pod UID、同一个 runc 容器进行 edgecore 重启前后对照：

| 项目 | 重启前 | 重启后 |
| --- | --- | --- |
| edgecore 进程启动时间 | 10:09:50 CST | 11:36:08 CST |
| 容器创建时间 | 10:31:23 CST | 不变 |
| 宿主机及 edgecore 的 projected 卷设备 | `0:44` | `0:261` |
| 容器内 projected 卷设备 | `0:44` | `0:44` |
| 容器内 token 与 MetaDB 精确匹配数 | 1 | 0 |
| 该 Pod 已缓存的 token 代 | 两代 | 仅重启后新生成的一代 |

容器内旧 JWT 仍未到期。当前 GC 按 JWT 有效期回收，无法解释这里的提前删除。

11:36:08 的日志顺序：

1. `.850953`：EdgeD 开始同步 Pod。
2. `.942482`：卷期望状态初始化宣告完成。
3. `.959399`：开始卸载上述仍运行 Pod 的 `kube-api-access` 卷。
4. `.982678`：该 projected 卷卸载成功。

同一窗口还批量卸载了其他 Pod 的 projected、ConfigMap、emptyDir 卷。
因此故障范围超出 ServiceAccount token 缓存本身。

## 代码路径

1. `edge/pkg/edged/edged.go` 发送初始 Pod `SET` 后调用 `SetInitPodReady(true)`。
   PodConfig 到 kubelet 的标准化更新通道带缓冲，发送完成不代表 PodManager 已处理。
2. 修复前，依赖中的 `PodConfig.SeenAllSources` 直接返回 `initPodReady`，
   忽略调用方传入的、由 kubelet 处理更新后记录的 `seenSources`。
3. 卷 populator 在 `sourcesReady.AllReady()` 为真时完成初始状态填充；此时
   PodManager 可能仍无存量 Pod，卷期望状态也可能为空。
4. reconciler 将重建的实际卷视为不再需要的卷，执行卸载。
5. projected 卷 `TearDownAt` 经 token manager/bridge 调用
   `DeleteServiceAccountToken(podUID)`，删除该 Pod 的所有缓存 token 代。
6. 后续 Pod 同步重建宿主机卷并获取新 token；已有容器继续引用旧 tmpfs。
   容器的旧 token 已不在 DB，MetaServer 因此拒绝认证。

现场已确认误卸载及其后果；确定性本地回归证明上述就绪判断存在提前放行窗口。
修复后的真实 Linux 进程重启仍需现场验证，不能用本地测试替代。

## 本地补丁

`vendor/k8s.io/kubernetes/pkg/kubelet/config/config.go` 保留初始 Pod 就绪标志，
同时检查：

- PodConfig 已收到所有配置源的初始 `SET`。
- kubelet 已处理来自所有配置源的更新。

配置源集合及快照标志使用各自的锁读取。空初始快照仍会发送空 `ADD`，
在 kubelet 确认处理后正常放行，不会永久阻塞空节点的清理。

回归位于同目录的 `readiness_test.go`，覆盖初始更新尚未消费、空节点、
尚未收到初始快照、多源尚未全部处理，以及清除初始就绪标志。

**依赖固化进展（2026-09-23 更新）：** 初次验证直接修改 vendor；随后用户已将
源码补丁及回归测试发布到 `github.com/iKelvinLab/kubeedge-kubernetes`
`v1.32.10-kubeedge3`，提交为 `7dfa01e85806df41b62cea77af1fa7356ca084b8`。
当前根模块及 staging API 的主模块替换关系均已接入 kubeedge3，并通过仓库脚本
重新生成 vendor。生成后的修复源码与发布模块完全一致，无需再手工修改 vendor。
独立 `k8s.io/kubelet` staging 模块本次没有源码变化，保留个人 fork 的 kubeedge2。

## 本轮验证

本机 Go/macOS 环境，使用 `GOWORK=off` 隔离父工作区的 replacement 冲突：

```bash
GOWORK=off go test k8s.io/kubernetes/pkg/kubelet/config \
  -run '^TestInitialPodReadiness' -count=1
GOWORK=off go test -race k8s.io/kubernetes/pkg/kubelet/config -count=1
GOWORK=off go vet k8s.io/kubernetes/pkg/kubelet/config
GOWORK=off go test ./edge/pkg/metamanager \
  ./edge/pkg/metamanager/metaserver/auth ./edge/pkg/metamanager/client \
  -run 'Test(HandleServiceAccountTokenResponse|AuthenticateExtendedTokenAfterStartupGC|ServiceAccountToken|NewestUnexpiredToken|TokenExists|GCExpiredTokensOnce|RunExpiredTokenGC)' \
  -count=1
```

- 修复前新增回归失败，明确提示 kubelet 尚未消费更新就已就绪。
- 修复后上述定向回归、竞态检查及 vet 通过。
- 未执行全量 CI、Linux 整机重启测试；未部署或提交。

## 现场复验与恢复

1. 构建时确保实际使用包含补丁的 vendor，例如关闭父工作区并显式使用
   `-mod=vendor`。使用 Linux 环境及项目所需的构建配置；本轮未生成部署二进制。
2. 在维护窗口部署修复后，对已受影响 Pod 安排重建，以重新挂载当前卷。
   防止未来误卸载的补丁不会自动修复已经分离的卷或恢复被删除的缓存。
3. 固定一个已恢复的 Pod UID/容器，采集重启前 token、卷和 DB 对照。
4. 仅重启 edgecore，再采集同一 UID/容器。验收：已有卷未被错误卸载，
   容器与宿主机仍指向同一文件系统，容器 token 精确匹配 DB，请求持续成功。
   token 正常轮转时可以变化，不要求字节永远不变。
5. 一并检查存量 ConfigMap、emptyDir 卷未被误清理。

已发生的 emptyDir 卸载可能影响临时数据；重建业务 Pod 前应按应用情况检查和保存
需要的数据。不要把手工卸载存量卷或将未知 token 写入 DB 当作修复步骤。

## 正式依赖接入验证（2026-09-23 后续）

- 用户已经确认此前 tokenData 修复现场验证通过；这与本次正式依赖版本接入分开记录。
- 发布版的 `config.go`、`readiness_test.go` 与此前本地补丁完全相同。
- `hack/update-vendor.sh` 成功执行；重复生成后 12510 项依赖内容/链接无变化。
- 新增回归通过临时 Go overlay 加载发布模块的测试文件，针对实际 vendor 代码运行，
  race 检查通过；token 三包定向回归及 config vet 通过。
- vendor 不保留依赖的 `_test.go` 文件是正常生成行为，测试由依赖源码版本维护。
- 根 go.mod、go.sum、staging API go.mod/go.sum、vendor 源码及 modules.txt 共六个相关文件变更。
- 已在 Go 1.23.12 / Linux amd64 Docker 环境离线构建 edgecore，CGO_ENABLED=1；
  产物元数据明确主模块 kubeedge3、staging kubelet kubeedge2。
- 新产物 SHA256：`79ae478d241a5db4d967124b05aa5e1fed004cd957a6405ffe4d559254e0689c`。
- 本次接入未提交、推送或部署；完整 CI 未执行。生成稳定性检查通过不等于
  `make verify-vendor` 的干净工作树条件已通过。
