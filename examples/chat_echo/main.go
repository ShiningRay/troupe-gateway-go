// 最小示例：echo 网关（对应 Crystal 版 examples/chat_echo.cr）。
// 用真实 troupego 运行时：EchoActor 的 beep turn 产事件 → Outbox 派发循环 → 广播。
// 运行：go run ./examples/chat_echo
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	gateway "github.com/ShiningRay/troupe-gateway-go"
	troupe "github.com/ShiningRay/troupe-go"
)

type echoProps struct {
	N int `json:"n"`
}

type echoActor struct {
	*troupe.Actor[echoProps]
}

func main() {
	ownership := troupe.NewMemoryOwnershipStore()
	store := troupe.NewMemoryOutboxStore(ownership)
	stage := troupe.NewStage(troupe.WithStore(store), troupe.WithOwnership(ownership), troupe.WithName("chat-echo"))

	manifest := troupe.Define("echo", func(id troupe.ActorID, props echoProps, actx *troupe.ActivationContext) *echoActor {
		return &echoActor{Actor: troupe.NewActor(id, props, actx)}
	})
	manifest.SchemaVersion(1)
	manifest.Initial(func() echoProps { return echoProps{} })
	manifest.Turn(troupe.Turn("beep", func(a *echoActor, ctx *troupe.CallContext, args struct{ Text string }) (int, error) {
		p := a.Props()
		p.N++
		if _, err := a.EmitEvent("beep", map[string]any{"n": p.N, "text": args.Text}); err != nil {
			return 0, err
		}
		if err := a.Save(); err != nil {
			return 0, err
		}
		return p.N, nil
	}))
	if err := manifest.Register(stage); err != nil {
		log.Fatal(err)
	}

	gw := gateway.NewWebSocketGateway(store, gateway.DefaultConfig())

	// 应用协议路由：上行原文 → actor 调用 → 回 echo
	gw.OnMessage = func(session *gateway.Session, raw string) {
		id := troupe.MustActorID("demo", "echo", "1")
		if future, err := stage.Call(context.Background(), id, "beep", struct{ Text string }{Text: raw}); err == nil {
			go func() { _, _ = future.Await(context.Background()) }()
		}
		_ = gw.Deliver(session, map[string]string{"type": "echo", "text": raw})
	}

	// Outbox 事件路由（已按 event_id 去重；至少一次由 troupe 保证）
	gw.OnOutbox = func(rec troupe.OutboxRecord) error {
		var payload map[string]any
		if err := json.Unmarshal(rec.Payload, &payload); err != nil {
			payload = map[string]any{}
		}
		payload["type"] = "event"
		payload["topic"] = rec.Topic
		gw.Broadcast(payload, "")
		return nil
	}

	id := troupe.MustActorID("demo", "echo", "1")
	gw.Track(id)
	// 预热：连接前先排队一条广播
	if future, err := stage.Call(context.Background(), id, "beep", struct{ Text string }{Text: "hello"}); err == nil {
		go func() { _, _ = future.Await(context.Background()) }()
	}

	port, err := gw.Listen("127.0.0.1", 7792)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("troupe-gateway chat_echo listening: ws://127.0.0.1:%d/\n", port)
	for {
		time.Sleep(time.Hour)
	}
}
