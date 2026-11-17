package gateway

// 测试客户端：手写最小 RFC 6455 客户端（net.Conn + 掩码帧），
// 支持自定义握手头（测试矩阵需要 X-Test 头）。

import (
	"bufio"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

type wsTestClient struct {
	t      *testing.T
	conn   net.Conn
	br     *bufio.Reader
	mu     sync.Mutex
	queue  [][]byte
	closed bool
}

func dialWS(t *testing.T, url string, headers map[string]string) *wsTestClient {
	t.Helper()
	hostPort := strings.TrimPrefix(strings.TrimPrefix(url, "ws://"), "http://")
	path := "/"
	if idx := strings.Index(hostPort, "/"); idx >= 0 {
		path = hostPort[idx:]
		hostPort = hostPort[:idx]
	}
	conn, err := net.DialTimeout("tcp", hostPort, 3*time.Second)
	if err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	key := make([]byte, 16)
	_, _ = rand.Read(key)
	req := "GET " + path + " HTTP/1.1\r\n" +
		"Host: " + hostPort + "\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Key: " + base64Key(key) + "\r\n" +
		"Sec-WebSocket-Version: 13\r\n"
	for k, v := range headers {
		req += k + ": " + v + "\r\n"
	}
	req += "\r\n"
	if _, err := conn.Write([]byte(req)); err != nil {
		t.Fatalf("握手写入失败: %v", err)
	}
	br := bufio.NewReader(conn)
	status, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("握手读取失败: %v", err)
	}
	if !strings.Contains(status, "101") {
		conn.Close()
		t.Fatalf("upgrade failed: %s", strings.TrimSpace(status))
	}
	// 吃掉剩余握手头
	for {
		line, rerr := br.ReadString('\n')
		if rerr != nil || line == "\r\n" {
			break
		}
	}
	c := &wsTestClient{t: t, conn: conn, br: br}
	go c.readLoop()
	return c
}

func base64Key(b []byte) string {
	const tbl = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
	var out strings.Builder
	for i := 0; i < len(b); i += 3 {
		var v uint32
		n := len(b) - i
		if n > 3 {
			n = 3
		}
		for j := 0; j < n; j++ {
			v |= uint32(b[i+j]) << (8 * (2 - j))
		}
		out.WriteByte(tbl[(v>>18)&0x3f])
		out.WriteByte(tbl[(v>>12)&0x3f])
		if n >= 2 {
			out.WriteByte(tbl[(v>>6)&0x3f])
		} else {
			out.WriteByte('=')
		}
		if n >= 3 {
			out.WriteByte(tbl[v&0x3f])
		} else {
			out.WriteByte('=')
		}
	}
	return out.String()
}

func (c *wsTestClient) sendRaw(raw string) {
	c.writeMasked(opText, []byte(raw))
}

func (c *wsTestClient) close() {
	if c.closed {
		return
	}
	c.closed = true
	c.writeMasked(opClose, []byte{0x03, 0xe8})
	_ = c.conn.Close()
}

// expect 等待第一条满足条件的消息（JSON 解析；不匹配的按到达顺序丢弃），超时 fail。
func (c *wsTestClient) expect(timeout time.Duration, pred func(m map[string]any) bool) map[string]any {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			c.t.Fatal("等待消息超时")
		}
		raw, ok := c.next(remaining)
		if !ok {
			if c.IsClosed() {
				c.t.Fatal("连接已关闭")
			}
			c.t.Fatal("等待消息超时")
		}
		var m map[string]any
		if err := json.Unmarshal(raw, &m); err != nil {
			c.t.Fatalf("JSON 解析失败: %v (%s)", err, raw)
		}
		if pred(m) {
			return m
		}
	}
}

// IsClosed 连接是否已关闭。
func (c *wsTestClient) IsClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// next 下一条原始消息；关闭/超时返回 (nil, false)。（测试内轮询实现，无等待者竞态）
func (c *wsTestClient) next(timeout time.Duration) ([]byte, bool) {
	deadline := time.Now().Add(timeout)
	for {
		c.mu.Lock()
		if len(c.queue) > 0 {
			raw := c.queue[0]
			c.queue = c.queue[1:]
			c.mu.Unlock()
			return raw, true
		}
		c.mu.Unlock()
		if time.Now().After(deadline) {
			return nil, false
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (c *wsTestClient) readLoop() {
	for {
		fin, op, payload, err := c.readFrame()
		if err != nil {
			c.mu.Lock()
			c.closed = true
			c.mu.Unlock()
			return
		}
		switch op {
		case opClose:
			c.mu.Lock()
			c.closed = true
			c.mu.Unlock()
			return
		case opText:
			if fin {
				c.mu.Lock()
				c.queue = append(c.queue, payload)
				c.mu.Unlock()
			}
		}
	}
}

func (c *wsTestClient) readFrame() (bool, byte, []byte, error) {
	var head [2]byte
	if _, err := readFull(c.br, head[:]); err != nil {
		return false, 0, nil, err
	}
	fin := head[0]&0x80 != 0
	op := head[0] & 0x0f
	length := uint64(head[1] & 0x7f)
	if length == 126 {
		var ext [2]byte
		if _, err := readFull(c.br, ext[:]); err != nil {
			return false, 0, nil, err
		}
		length = uint64(binary.BigEndian.Uint16(ext[:]))
	} else if length == 127 {
		var ext [8]byte
		if _, err := readFull(c.br, ext[:]); err != nil {
			return false, 0, nil, err
		}
		length = binary.BigEndian.Uint64(ext[:])
	}
	payload := make([]byte, length)
	if length > 0 {
		if _, err := readFull(c.br, payload); err != nil {
			return false, 0, nil, err
		}
	}
	return fin, op, payload, nil
}

func readFull(br *bufio.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := br.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func (c *wsTestClient) writeMasked(op byte, payload []byte) {
	c.t.Helper()
	mask := make([]byte, 4)
	_, _ = rand.Read(mask)
	masked := make([]byte, len(payload))
	for i, b := range payload {
		masked[i] = b ^ mask[i%4]
	}
	length := len(masked)
	var head []byte
	if length < 126 {
		head = []byte{0x80 | op, 0x80 | byte(length)}
	} else if length < 65536 {
		head = []byte{0x80 | op, 0x80 | 126, 0, 0}
		binary.BigEndian.PutUint16(head[2:], uint16(length))
	} else {
		head = []byte{0x80 | op, 0x80 | 127, 0, 0, 0, 0, 0, 0, 0, 0}
		binary.BigEndian.PutUint64(head[2:], uint64(length))
	}
	frame := append(append(head, mask...), masked...)
	if _, err := c.conn.Write(frame); err != nil && !c.closed {
		c.t.Fatalf("写帧失败: %v", err)
	}
}

// waitUntil 轮询等待条件成立（10ms 步进）。
func waitUntil(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("waitUntil 超时")
}
