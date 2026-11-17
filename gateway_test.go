package gateway

// 传输桥语义（移植 troupe-gateway Crystal spec/web_socket_gateway_spec.cr）：
// 路由回调、会话元数据、投递/广播、溢出策略、断线清理、static_dir、去重。

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ShiningRay/troupe-go"
)

func newTestGateway(t *testing.T, mutate func(*Config)) (*WebSocketGateway, int) {
	t.Helper()
	cfg := DefaultConfig()
	if mutate != nil {
		mutate(&cfg)
	}
	g := NewWebSocketGateway(nil, cfg)
	port, err := g.Listen("127.0.0.1", 0)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(g.Stop)
	return g, port
}

func TestOnMessageRoutingAndHandshake(t *testing.T) {
	g, port := newTestGateway(t, nil)
	type hit struct{ id, path, header, raw string }
	got := make(chan hit, 4)

	g.OnMessage = func(s *Session, raw string) {
		got <- hit{s.ID, s.Path, s.Headers.Get("X-Test"), raw}
		if !g.Deliver(s, map[string]string{"type": "pong"}) {
			t.Error("deliver 失败")
		}
	}

	c := dialWS(t, fmt.Sprintf("ws://127.0.0.1:%d/ws?x=1", port), map[string]string{"X-Test": "probe"})
	c.sendRaw(`{"type":"ping"}`)

	h := <-got
	if h.id == "" {
		t.Error("session id 为空")
	}
	if h.path != "/ws" {
		t.Errorf("path = %q, want /ws", h.path)
	}
	if h.header != "probe" {
		t.Errorf("header = %q, want probe", h.header)
	}
	if h.raw != `{"type":"ping"}` {
		t.Errorf("raw = %q", h.raw)
	}

	reply := c.expect(3*time.Second, func(m map[string]any) bool { return m["type"] == "pong" })
	if reply["type"] != "pong" {
		t.Errorf("reply = %v", reply)
	}
}

func TestOnConnectRejection(t *testing.T) {
	g, port := newTestGateway(t, nil)
	g.OnConnect = func(s *Session) bool { return s.Meta["try"] == "ok" }
	var mu sync.Mutex
	var disconnected []string
	g.OnDisconnect = func(s *Session) {
		mu.Lock()
		disconnected = append(disconnected, s.ID)
		mu.Unlock()
	}

	rejected := dialWS(t, fmt.Sprintf("ws://127.0.0.1:%d/", port), nil)
	rejected.sendRaw(`{"type":"ping"}`)
	time.Sleep(250 * time.Millisecond)
	if g.SessionsSize() != 0 {
		t.Errorf("sessions = %d, want 0", g.SessionsSize())
	}
	mu.Lock()
	defer mu.Unlock()
	if len(disconnected) != 0 {
		t.Errorf("拒绝接入不应触发 on_disconnect: %v", disconnected)
	}
}

func TestDeliverBroadcastAndDisconnectCleanup(t *testing.T) {
	g, port := newTestGateway(t, nil)
	var mu sync.Mutex
	disconnects := make(chan string, 8)
	g.OnDisconnect = func(s *Session) { disconnects <- s.ID }
	ids := map[string]string{}

	g.OnMessage = func(s *Session, raw string) {
		mu.Lock()
		ids[raw] = s.ID
		mu.Unlock()
		g.Broadcast(map[string]string{"type": "b", "to": raw}, "")
	}

	a := dialWS(t, fmt.Sprintf("ws://127.0.0.1:%d/", port), nil)
	b := dialWS(t, fmt.Sprintf("ws://127.0.0.1:%d/", port), nil)
	t.Cleanup(b.close)

	a.sendRaw(`"A"`)
	waitUntil(t, 3*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return ids[`"A"`] != ""
	})
	mu.Lock()
	aID := ids[`"A"`]
	mu.Unlock()
	b.sendRaw(`"B"`)
	waitUntil(t, 3*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return ids[`"B"`] != ""
	})

	time.Sleep(150 * time.Millisecond)
	if got := g.SessionsSize(); got != 2 {
		t.Fatalf("sessions = %d, want 2", got)
	}

	if n := g.Broadcast(map[string]string{"type": "b"}, ""); n != 2 {
		t.Errorf("broadcast = %d, want 2", n)
	}
	a.expect(3*time.Second, func(m map[string]any) bool { return m["type"] == "b" })
	b.expect(3*time.Second, func(m map[string]any) bool { return m["type"] == "b" })

	if !g.DeliverID(aID, map[string]string{"type": "direct"}) {
		t.Error("deliver_id 应成功")
	}
	a.expect(3*time.Second, func(m map[string]any) bool { return m["type"] == "direct" })
	if g.DeliverID("nonexistent", map[string]string{"type": "direct"}) {
		t.Error("未知 id 应返回 false")
	}

	// 广播排除语义
	if n := g.Broadcast(map[string]string{"type": "b2"}, aID); n != 1 {
		t.Errorf("broadcast except = %d, want 1", n)
	}
	b.expect(3*time.Second, func(m map[string]any) bool { return m["type"] == "b2" })

	// a 断线：on_disconnect 触发并清理
	a.close()
	if got := <-disconnects; got != aID {
		t.Errorf("disconnect id = %q, want %q", got, aID)
	}
	waitUntil(t, 3*time.Second, func() bool { return g.SessionsSize() == 1 })
}

func TestStaticDirAndFallback(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "index.html"), []byte("<h1>ok</h1>"), 0o644); err != nil {
		t.Fatal(err)
	}
	g, port := newTestGateway(t, func(c *Config) { c.StaticDir = dir })

	res, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/", port))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != 200 {
		t.Errorf("GET / = %d, want 200", res.StatusCode)
	}
	if string(body) != "<h1>ok</h1>" {
		t.Errorf("body = %q", body)
	}
	if ct := res.Header.Get("Content-Type"); ct != "text/html; charset=utf-8" {
		t.Errorf("content-type = %q", ct)
	}

	res2, _ := http.Get(fmt.Sprintf("http://127.0.0.1:%d/missing.js", port))
	res2.Body.Close()
	if res2.StatusCode != 404 {
		t.Errorf("GET /missing.js = %d, want 404", res2.StatusCode)
	}
	if ct := res2.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("404 content-type = %q", ct)
	}

	res3, _ := http.Get(fmt.Sprintf("http://127.0.0.1:%d/../secret.txt", port))
	res3.Body.Close()
	if res3.StatusCode != 404 {
		t.Errorf("traversal = %d, want 404", res3.StatusCode)
	}
	_ = g
}

func TestOverflowDrop(t *testing.T) {
	g := NewWebSocketGateway(nil, func() Config {
		c := DefaultConfig()
		c.MailboxCap = 2
		c.PushWait = 50 * time.Millisecond
		c.Overflow = OverflowDrop
		return c
	}())
	s := newSession("s1", 2, "", nil, nil)
	g.Register(s)

	if !g.Deliver(s, `{"n":1}`) {
		t.Error("deliver 1 应成功")
	}
	if !g.Deliver(s, `{"n":2}`) {
		t.Error("deliver 2 应成功")
	}
	if g.Deliver(s, `{"n":3}`) {
		t.Error("deliver 3 应溢出失败")
	}
	if g.SessionsSize() != 1 {
		t.Errorf("sessions = %d, want 1", g.SessionsSize())
	}
	g.Stop()
}

func TestOverflowDisconnect(t *testing.T) {
	g := NewWebSocketGateway(nil, func() Config {
		c := DefaultConfig()
		c.MailboxCap = 2
		c.PushWait = 50 * time.Millisecond
		c.Overflow = OverflowDisconnect
		return c
	}())
	disconnects := make(chan string, 4)
	g.OnDisconnect = func(s *Session) { disconnects <- s.ID }
	s := newSession("s2", 2, "", nil, nil)
	g.Register(s)

	if !g.Deliver(s, `{"n":1}`) {
		t.Error("deliver 1 应成功")
	}
	if !g.Deliver(s, `{"n":2}`) {
		t.Error("deliver 2 应成功")
	}
	if g.Deliver(s, `{"n":3}`) {
		t.Error("deliver 3 应溢出失败")
	}
	if got := <-disconnects; got != "s2" {
		t.Errorf("disconnect id = %q, want s2", got)
	}
	if g.SessionsSize() != 0 {
		t.Errorf("sessions = %d, want 0", g.SessionsSize())
	}
	if !s.ClosedMailbox() {
		t.Error("mailbox 应已关闭")
	}
	g.Stop()
}

func TestDuplicateDedup(t *testing.T) {
	g := NewWebSocketGateway(nil, DefaultConfig())
	if g.Duplicate("e1") {
		t.Error("首次不应重复")
	}
	if !g.Duplicate("e1") {
		t.Error("第二次应重复")
	}
	if g.Duplicate("e2") {
		t.Error("新 id 不应重复")
	}
	g.Stop()

	g2 := NewWebSocketGateway(nil, func() Config {
		c := DefaultConfig()
		c.Dedup = false
		return c
	}())
	if g2.Duplicate("e1") || g2.Duplicate("e1") {
		t.Error("dedup=false 应透传")
	}
	g2.Stop()
}

// Outbox 派发循环（移植 Crystal spec/outbox_dispatch_spec.cr）：
// 真实 troupego 运行时 + MemoryOutboxStore + emitter actor → track → OnOutbox。
type emitterProps struct {
	N int `json:"n"`
}

type emitterActor struct {
	*troupe.Actor[emitterProps]
}

func TestOutboxDispatch(t *testing.T) {
	ownership := troupe.NewMemoryOwnershipStore()
	store := troupe.NewMemoryOutboxStore(ownership)
	stage := troupe.NewStage(troupe.WithStore(store), troupe.WithOwnership(ownership), troupe.WithName("gw-test"))

	manifest := troupe.Define("emitter", func(id troupe.ActorID, props emitterProps, actx *troupe.ActivationContext) *emitterActor {
		return &emitterActor{Actor: troupe.NewActor(id, props, actx)}
	})
	manifest.SchemaVersion(1)
	manifest.Initial(func() emitterProps { return emitterProps{} })
	manifest.Turn(troupe.Turn("emit", func(a *emitterActor, ctx *troupe.CallContext, _ struct{}) (int, error) {
		p := a.Props()
		p.N++
		if _, err := a.EmitEvent("ping", map[string]int{"n": p.N}); err != nil {
			return 0, err
		}
		if err := a.Save(); err != nil {
			return 0, err
		}
		return p.N, nil
	}))
	if err := manifest.Register(stage); err != nil {
		t.Fatal(err)
	}

	cfg := DefaultConfig()
	cfg.DispatchInterval = 25 * time.Millisecond
	g := NewWebSocketGateway(store, cfg)
	records := make(chan troupe.OutboxRecord, 16)
	g.OnOutbox = func(rec troupe.OutboxRecord) error {
		records <- rec
		return nil
	}
	if _, err := g.Listen("127.0.0.1", 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(g.Stop)

	id := troupe.MustActorID("gw", "emitter", "1")
	g.Track(id)
	if g.TrackedSize() != 1 {
		t.Fatalf("tracked = %d, want 1", g.TrackedSize())
	}

	future, err := stage.Call(t.Context(), id, "emit", struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := future.Await(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var n int
	if err := json.Unmarshal(raw, &n); err != nil || n != 1 {
		t.Fatalf("emit = %s (err %v), want 1", raw, err)
	}

	var rec troupe.OutboxRecord
	select {
	case rec = <-records:
	case <-time.After(3 * time.Second):
		t.Fatal("等待 Outbox 事件超时")
	}
	if rec.Topic != "ping" {
		t.Errorf("topic = %q, want ping", rec.Topic)
	}
	var payload map[string]int
	if err := json.Unmarshal(rec.Payload, &payload); err != nil || payload["n"] != 1 {
		t.Errorf("payload = %s, want n=1", rec.Payload)
	}

	g.Untrack(id)
	if g.TrackedSize() != 0 {
		t.Errorf("tracked = %d, want 0", g.TrackedSize())
	}
}
