// Package gateway 实现 Troupe 的外部端点传输桥（ADR-0007 L3）。
//
// 职责（仅传输管理，协议路由归应用）：
//   - WS 会话生命周期（握手信息暴露、on_connect 接纳、断线清理）；
//   - 出站投递：每会话有界 mailbox + 限时入队 + 溢出策略（Drop/Disconnect）；
//   - Outbox 派发循环：track 的 actor 每 DispatchInterval 轮询一次，
//     事件（至少一次）经可选 event_id 去重后交 OnOutbox 由应用路由；
//   - 非 WS 请求的静态目录/404 JSON 兜底。
//
// 不负责：应用协议路由、业务语义、认证授权、路由/中间件。
package gateway

import "time"

// OverflowPolicy mailbox 溢出策略。
type OverflowPolicy int

const (
	// OverflowDrop 丢弃该条消息，会话保留（在线尽力，默认）。
	OverflowDrop OverflowPolicy = iota
	// OverflowDisconnect 丢弃并断开会话（防慢消费者长期占用）。
	OverflowDisconnect
)

// Config 网关配置（与 troupe-gateway Crystal 版 Config 对齐）。
type Config struct {
	// MailboxCap 每会话出站 mailbox 容量。
	MailboxCap int
	// PushWait 限时入队等待；超时视为溢出。
	PushWait time.Duration
	// Overflow 溢出策略。
	Overflow OverflowPolicy
	// Dedup event_id 去重开关。
	Dedup bool
	// DedupCache 去重集合容量（超出整表清空，与 Crystal 版一致）。
	DedupCache int
	// DispatchInterval Outbox 派发循环间隔。
	DispatchInterval time.Duration
	// PollBatch 派发循环单次 poll 的事件批量。
	PollBatch int
	// ServiceName 404 兜底应答中的服务名。
	ServiceName string
	// StaticDir 可选：同端口托管静态目录（非 WS 请求命中文件回文件，否则 404 JSON）。
	StaticDir string
}

// DefaultConfig 返回默认配置（数值对齐 Crystal 版）。
func DefaultConfig() Config {
	return Config{
		MailboxCap:       256,
		PushWait:         200 * time.Millisecond,
		Overflow:         OverflowDrop,
		Dedup:            true,
		DedupCache:       4096,
		DispatchInterval: 50 * time.Millisecond,
		PollBatch:        64,
		ServiceName:      "troupe-gateway",
		StaticDir:        "",
	}
}
