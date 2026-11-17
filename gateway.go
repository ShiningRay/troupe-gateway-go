package gateway

// WebSocket 网关：外部客户端 ↔ actor 的传输桥（ADR-0007 L3）。
// 语义对齐 troupe-gateway（Crystal shard）v0.1.3。

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/ShiningRay/troupe-go"
)

var errMessageTooLong = errors.New("消息超长")

// OutboxHandler Outbox 事件回调；返回 error 则不 mark-sent（下轮重投，至少一次）。
type OutboxHandler func(rec troupe.OutboxRecord) error

// WebSocketGateway 外部客户端 ↔ actor 的传输桥。
type WebSocketGateway struct {
	Config Config
	store  troupe.OutboxStore

	// ---- 应用回调 ----
	// OnMessage 上行原文回调（协议路由归应用）。
	OnMessage func(session *Session, raw string)
	// OnConnect 接纳钩子；返回 false 拒绝（会话随即关闭，不触发 OnDisconnect）。nil 视为接纳。
	OnConnect func(session *Session) bool
	// OnDisconnect 断线清理回调。
	OnDisconnect func(session *Session)
	// OnOutbox Outbox 事件回调（去重后；返回 error 则下轮重投）。
	OnOutbox OutboxHandler

	mu        sync.Mutex
	sessions  map[string]*Session
	tracked   map[string]troupe.ActorID
	seenEvent map[string]struct{}

	listener net.Listener
	server   *http.Server
	stopped  bool
	stopOnce sync.Once
}

// NewWebSocketGateway 构造网关。store 用于 Outbox 派发（如 troupe.NewMemoryOutboxStore）。
func NewWebSocketGateway(store troupe.OutboxStore, config Config) *WebSocketGateway {
	return &WebSocketGateway{
		Config:    config,
		store:     store,
		sessions:  map[string]*Session{},
		tracked:   map[string]troupe.ActorID{},
		seenEvent: map[string]struct{}{},
	}
}

// ---- actor 注册（Outbox 派发循环的轮询对象，逐 actor） ----

// Track 注册一个参与派发轮询的 actor。
func (g *WebSocketGateway) Track(id troupe.ActorID) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.tracked[id.Canonical()] = id
}

// Untrack 注销。
func (g *WebSocketGateway) Untrack(id troupe.ActorID) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.tracked, id.Canonical())
}

// TrackedSize 当前 tracked 数量。
func (g *WebSocketGateway) TrackedSize() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.tracked)
}

// ---- 会话注册（正常会话自动注册；供测试/应用造会话） ----

// Register 注册手工构造的会话（溢出策略测试用）。
func (g *WebSocketGateway) Register(s *Session) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.sessions[s.ID] = s
}

// Unregister 注销会话。
func (g *WebSocketGateway) Unregister(id string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.sessions, id)
}

// SessionsSize 当前会话数。
func (g *WebSocketGateway) SessionsSize() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.sessions)
}

// Session 按会话 id 查询（应用路由读取 meta/握手信息用）。
func (g *WebSocketGateway) Session(id string) *Session {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.sessions[id]
}

// ---- 启停 ----

// Listen 绑定并后台监听；返回实际端口（0 → 系统分配）。
func (g *WebSocketGateway) Listen(host string, port int) (int, error) {
	g.mu.Lock()
	if g.stopped {
		g.mu.Unlock()
		return 0, errors.New("gateway stopped")
	}
	g.mu.Unlock()

	go g.dispatchLoop()

	mux := http.NewServeMux()
	mux.HandleFunc("/", g.handleHTTP)
	srv := &http.Server{Handler: mux}
	ln, err := net.Listen("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		return 0, err
	}
	g.mu.Lock()
	g.listener = ln
	g.server = srv
	g.mu.Unlock()
	go func() { _ = srv.Serve(ln) }()
	return ln.Addr().(*net.TCPAddr).Port, nil
}

// Stop 停服停派发循环（幂等）。
func (g *WebSocketGateway) Stop() {
	g.stopOnce.Do(func() {
		g.mu.Lock()
		g.stopped = true
		srv := g.server
		ln := g.listener
		g.mu.Unlock()
		if srv != nil {
			_ = srv.Close()
		}
		if ln != nil {
			_ = ln.Close()
		}
	})
}

// ---- 下行投递 ----

// Deliver 投递给会话：有界 mailbox + 限时入队，超时按溢出策略处理。返回 true 表示已入队。
func (g *WebSocketGateway) Deliver(s *Session, payload any) bool {
	if s == nil {
		return false
	}
	raw, ok := payloadToString(payload)
	if !ok {
		return false
	}
	wait := time.After(g.Config.PushWait)
	delivered := s.mailboxSend(wsFrame{op: opText, data: []byte(raw)}, wait)
	if !delivered && !s.ClosedMailbox() {
		g.overflow(s)
	}
	return delivered
}

// DeliverID 按会话 id 投递；会话不存在返回 false。
func (g *WebSocketGateway) DeliverID(id string, payload any) bool {
	return g.Deliver(g.Session(id), payload)
}

// Broadcast 广播到全部会话（可排除一个 id）；返回入队成功的会话数。
func (g *WebSocketGateway) Broadcast(payload any, except string) int {
	g.mu.Lock()
	ids := make([]string, 0, len(g.sessions))
	for id := range g.sessions {
		ids = append(ids, id)
	}
	g.mu.Unlock()
	count := 0
	for _, id := range ids {
		if id == except {
			continue
		}
		if g.DeliverID(id, payload) {
			count++
		}
	}
	return count
}

// Duplicate 事件去重查询（至少一次 → 消费侧 effectively-once；溢出整表清空）。
func (g *WebSocketGateway) Duplicate(eventID string) bool {
	if !g.Config.Dedup {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.seenEvent[eventID]; ok {
		return true
	}
	g.seenEvent[eventID] = struct{}{}
	if len(g.seenEvent) > g.Config.DedupCache {
		g.seenEvent = map[string]struct{}{}
	}
	return false
}

// ---- 内部 ----

func payloadToString(payload any) (string, bool) {
	switch v := payload.(type) {
	case string:
		return v, true
	case []byte:
		return string(v), true
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return "", false
		}
		return string(b), true
	}
}

// overflow 溢出策略：Drop 丢弃该条；Disconnect 踢除会话（幂等，对齐 Crystal 版）。
func (g *WebSocketGateway) overflow(s *Session) {
	if g.Config.Overflow != OverflowDisconnect {
		return
	}
	func() {
		defer func() { _ = recover() }() // 溢出路径吞错（对齐 Crystal 版）
		g.Unregister(s.ID)
		s.markCleaned()
		s.mailboxClose()
		if g.OnDisconnect != nil {
			g.OnDisconnect(s)
		}
		if s.socket != nil {
			s.socket.writeCloseFrame(1008, "slow consumer")
			s.socket.close()
		}
	}()
}

// handleHTTP 非 WS 请求兜底 + WS 升级入口。
func (g *WebSocketGateway) handleHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.EqualFold(r.Header.Get("Upgrade"), "websocket") && r.Header.Get("Sec-WebSocket-Version") == "13" {
		g.handleUpgrade(w, r)
		return
	}
	g.handleFallback(w, r)
}

func (g *WebSocketGateway) handleUpgrade(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Sec-WebSocket-Key")
	if key == "" {
		http.Error(w, "bad handshake", http.StatusBadRequest)
		return
	}
	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "hijack unsupported", http.StatusInternalServerError)
		return
	}
	conn, brw, err := hj.Hijack()
	if err != nil {
		return
	}
	if err := writeHandshakeResponse(conn, acceptKey(key)); err != nil {
		_ = conn.Close()
		return
	}
	c := newWsConn(conn, brw.Reader)
	g.runSession(c, r)
}

// runSession 单连接生命周期：握手信息 → 接纳钩子 → 注册 → 写循环 + 读循环。
func (g *WebSocketGateway) runSession(c *wsConn, r *http.Request) {
	buf := make([]byte, 16)
	_, _ = rand.Read(buf)
	id := hex.EncodeToString(buf)

	query := map[string]string{}
	for k, vs := range r.URL.Query() {
		if len(vs) > 0 {
			query[k] = vs[0]
		}
	}
	session := newSession(id, g.Config.MailboxCap, r.URL.Path, query, r.Header.Clone())
	session.socket = c

	if g.OnConnect != nil && !g.OnConnect(session) {
		c.writeCloseFrame(1008, "rejected") // 拒接入：不注册、不触发 OnDisconnect
		c.close()
		return
	}

	g.Register(session)

	// 写循环：独占 socket 发送；mailbox 关闭后排空退出。
	go func() {
		for f := range session.mailbox {
			if err := c.writeFrame(f.op, f.data); err != nil {
				return
			}
		}
	}()

	g.readLoop(session, c)

	// 断线路径：cleanup（幂等）→ 关 mailbox（写循环排空退出）。
	g.cleanup(session)
	session.mailboxClose()
	c.close()
}

func (g *WebSocketGateway) readLoop(s *Session, c *wsConn) {
	var fragments [][]byte
	fragText := false
	for {
		fin, op, payload, err := c.readFrame()
		if err != nil {
			if errors.Is(err, errMessageTooLong) {
				c.writeCloseFrame(1009, "too long")
			}
			return
		}
		switch op {
		case opClose:
			s.mailboxTrySend(wsFrame{op: opClose, data: payload})
			return
		case opPing:
			s.mailboxTrySend(wsFrame{op: opPong, data: payload})
		case opText, opBinary:
			if !fin {
				fragments = [][]byte{payload}
				fragText = op == opText
				continue
			}
			g.deliverUpstream(s, op, payload)
		case opCont:
			fragments = append(fragments, payload)
			total := 0
			for _, f := range fragments {
				total += len(f)
			}
			if total > maxMessage {
				c.writeCloseFrame(1009, "too long")
				return
			}
			if fin {
				full := make([]byte, 0, total)
				for _, f := range fragments {
					full = append(full, f...)
				}
				fragments = nil
				if fragText {
					g.deliverUpstream(s, opText, full)
				}
			}
		}
	}
}

func (g *WebSocketGateway) deliverUpstream(s *Session, op byte, payload []byte) {
	if op != opText || g.OnMessage == nil {
		return
	}
	g.OnMessage(s, string(payload))
}

// cleanup 幂等清理：注销 + OnDisconnect（恰一次）。
func (g *WebSocketGateway) cleanup(s *Session) {
	if !s.markCleaned() {
		return
	}
	g.Unregister(s.ID)
	if g.OnDisconnect != nil {
		g.OnDisconnect(s)
	}
}

// dispatchLoop Outbox 派发循环：锁内快照 tracked、锁外逐 actor 轮询；
// 回调不返回 error 才 mark-sent（至少一次 + 消费侧去重 = effectively-once）。
func (g *WebSocketGateway) dispatchLoop() {
	ctx := context.Background()
	for {
		g.mu.Lock()
		stopped := g.stopped
		ids := make([]troupe.ActorID, 0, len(g.tracked))
		for _, id := range g.tracked {
			ids = append(ids, id)
		}
		g.mu.Unlock()
		if stopped {
			return
		}
		for _, id := range ids {
			records, err := g.store.PollOutbox(ctx, id, g.Config.PollBatch)
			if err != nil {
				continue // 轮询失败静默，下轮重试
			}
			for _, rec := range records {
				if g.Duplicate(rec.EventID) {
					// 重复事件：照常标记（至少一次 + 去重 = effectively-once）
					g.store.MarkSent(ctx, id, rec.EventID)
					continue
				}
				if g.OnOutbox != nil && g.OnOutbox(rec) != nil {
					continue // 处理失败不 mark-sent → 下轮重投
				}
				g.store.MarkSent(ctx, id, rec.EventID)
			}
		}
		time.Sleep(g.Config.DispatchInterval)
	}
}

// handleFallback 非 WebSocket 请求的兜底应答：配置 StaticDir 时尝试回静态文件，否则 404 JSON。
func (g *WebSocketGateway) handleFallback(w http.ResponseWriter, r *http.Request) {
	if dir := g.Config.StaticDir; dir != "" {
		p := r.URL.Path
		if p == "/" {
			p = "/index.html"
		}
		file := strings.TrimPrefix(p, "/")
		// 路径安全：拒绝目录穿越
		if !strings.Contains(file, "..") {
			full := filepath.Join(dir, file)
			if st, err := os.Stat(full); err == nil && !st.IsDir() {
				body, rerr := os.ReadFile(full)
				if rerr == nil {
					w.Header().Set("Content-Type", mime.TypeByExtension(filepath.Ext(full)))
					_, _ = w.Write(body)
					return
				}
			}
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNotFound)
	_ = json.NewEncoder(w).Encode(map[string]string{"service": g.Config.ServiceName})
}

// 供 wsConn 读取使用（io 前向引用占位，防止未使用 import）。
var _ = io.EOF
