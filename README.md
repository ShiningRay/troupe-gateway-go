# troupe-gateway-go

Troupe 的**外部端点传输桥**（[troupe-gateway](https://github.com/ShiningRay/troupe-gateway) 的 Go 实现）：把"外部客户端 ↔ actor"的通用传输管理收编成开箱即用的伴生模块，协议路由留给应用。分层边界与决策记录见 [troupe ADR-0007](https://github.com/ShiningRay/troupe/blob/main/docs/adr/0007-external-endpoint-layering.md)。

## 职责边界（ADR-0007 L3）

**负责**：WS 会话生命周期、出站有界 mailbox + 限时投递 + 溢出策略（Drop/Disconnect）、Outbox 派发循环、`event_id` 去重、握手信息暴露。

**不负责**：应用协议路由、业务语义（房间/昵称）、认证授权、路由/中间件/序列化（交给 gin/chi/echo 等）。

## 快速开始

```go
import (
    troupe "github.com/ShiningRay/troupe-go"
    gateway "github.com/ShiningRay/troupe-gateway-go"
)

store := troupe.NewMemoryOutboxStore(ownership)      // 或 troupepg 的 OutboxStore
gw := gateway.NewWebSocketGateway(store, gateway.DefaultConfig())

// 应用协议路由：上行消息 → actor 调用
gw.OnMessage = func(session *gateway.Session, raw string) {
    // 解析 raw → stage.Call(...) → gw.Deliver(session, 回复)
}

// Outbox 事件路由（已按 event_id 去重；至少一次由 troupe 保证）
gw.OnOutbox = func(rec troupe.OutboxRecord) error {
    // 按业务把 rec.Payload 投给相关会话：gw.DeliverID / Broadcast；nil 表示已处理
    return nil
}

gw.Track(troupe.MustActorID("chat", "room", "lobby")) // 参与派发轮询
port, _ := gw.Listen("127.0.0.1", 7777)
```

## API

| 成员 | 说明 |
|---|---|
| `NewWebSocketGateway(store troupe.OutboxStore, config Config)` | store 用于 Outbox 派发 |
| `OnMessage(*Session, string)` | 上行原文回调 |
| `OnConnect(*Session) bool` | 接纳钩子，false 拒绝（不触发 OnDisconnect）；nil 视为接纳 |
| `OnDisconnect(*Session)` | 断线清理回调 |
| `OnOutbox(troupe.OutboxRecord) error` | Outbox 事件回调（去重后；返回 error 则下轮重投） |
| `Track / Untrack(troupe.ActorID)` | 派发循环的轮询对象（逐 actor，与 Crystal 版一致） |
| `Deliver(*Session, any) bool` | 限时投递；溢出按策略（string 直发，其余 JSON 序列化） |
| `DeliverID(id string, payload any) bool` | 按会话 id 投递 |
| `Broadcast(payload any, except string) int` | 全体广播（可排除），返回成功数 |
| `Duplicate(eventID string) bool` | 事件去重查询 |
| `Listen(host string, port int) (int, error)` / `Stop()` | 启停（listen 返回实际端口，0 → 系统分配） |
| `Session` | ID / Meta / Path / Query / Headers |

`Config`：`MailboxCap`（默认 256）、`PushWait`（200ms）、`Overflow`（`OverflowDrop` / `OverflowDisconnect`）、`Dedup`（true）、`DedupCache`（4096，超出整表清空）、`DispatchInterval`（50ms）、`PollBatch`（64）、`ServiceName`、`StaticDir`。

## 与 Crystal 版的差异

| 项 | Crystal（troupe-gateway） | 本实现 |
|---|---|---|
| 并发模型 | Fiber + Channel + Mutex | goroutine + channel + sync.Mutex |
| WS 实现 | Crystal stdlib `HTTP::WebSocket` | 零依赖 RFC 6455（net/http Hijack + 手写帧编解码） |
| Outbox API | `OutboxDispatcher#poll_once` | 直接轮询 `troupe.OutboxStore`（`PollOutbox`/`MarkSent`），逐 actor、语义一致 |
| 失败处理 | 回调抛错 → 不 mark_sent → 下轮重 poll | `OnOutbox` 返回 error → 不 `MarkSent` → 下轮重投 |
| deliver 阻塞 | Fiber 挂起限时 send | goroutine 阻塞在 `select`（`PushWait` 超时） |

## 验证

```sh
go test -race ./...        # 路由/握手/广播/溢出/去重/断线/静态目录/Outbox 派发
gofmt -l . && go vet ./...
go run ./examples/chat_echo   # ws://127.0.0.1:7792/
```

## 关联

- 参照实现：[troupe-gateway（Crystal）](https://github.com/ShiningRay/troupe-gateway)
- 运行时：[troupe-go](https://github.com/ShiningRay/troupe-go)
- 决策记录：[ADR-0007 外部端点分层](https://github.com/ShiningRay/troupe/blob/main/docs/adr/0007-external-endpoint-layering.md)

## License

MIT，见 [LICENSE](LICENSE)。
