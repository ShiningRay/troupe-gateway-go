package gateway

// 零依赖 WebSocket（RFC 6455 最小实现，文本帧子集）：
// - 服务端握手（Sec-WebSocket-Accept）+ 文本帧收发 + ping/pong + close
// - 客户端帧必须 mask（协议要求），分片（continuation）支持
// - 不支持二进制/扩展（网关仅 JSON 文本）

import (
	"bufio"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
)

const (
	wsGUID          = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
	maxMessage      = 1024 * 1024
	opCont     byte = 0x0
	opText     byte = 0x1
	opBinary   byte = 0x2
	opClose    byte = 0x8
	opPing     byte = 0x9
	opPong     byte = 0xA
)

// wsConn 已握手连接：读帧（阻塞）、写帧（互斥）。
type wsConn struct {
	conn   net.Conn
	br     *bufio.Reader
	wmu    sync.Mutex
	closed bool
}

func newWsConn(conn net.Conn, br *bufio.Reader) *wsConn {
	return &wsConn{conn: conn, br: br}
}

func acceptKey(key string) string {
	h := sha1.New()
	h.Write([]byte(key + wsGUID))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

func writeHandshakeResponse(conn net.Conn, accept string) error {
	_, err := conn.Write([]byte(
		"HTTP/1.1 101 Switching Protocols\r\n" +
			"Upgrade: websocket\r\n" +
			"Connection: Upgrade\r\n" +
			"Sec-WebSocket-Accept: " + accept + "\r\n\r\n"))
	return err
}

// readFrame 读一帧；EOF/断开返回 io.EOF。文本/分片由调用方拼装。
func (c *wsConn) readFrame() (fin bool, op byte, payload []byte, err error) {
	var head [2]byte
	if _, err = io.ReadFull(c.br, head[:]); err != nil {
		return
	}
	fin = head[0]&0x80 != 0
	op = head[0] & 0x0f
	masked := head[1]&0x80 != 0
	length := uint64(head[1] & 0x7f)
	if length == 126 {
		var ext [2]byte
		if _, err = io.ReadFull(c.br, ext[:]); err != nil {
			return
		}
		length = uint64(binary.BigEndian.Uint16(ext[:]))
	} else if length == 127 {
		var ext [8]byte
		if _, err = io.ReadFull(c.br, ext[:]); err != nil {
			return
		}
		length = binary.BigEndian.Uint64(ext[:])
	}
	if op >= opClose {
		if length > 125 {
			err = errors.New("控制帧负载超过 125 字节")
			return
		}
	} else if length > maxMessage {
		err = errMessageTooLong
		return
	}
	var mask []byte
	if masked {
		mask = make([]byte, 4)
		if _, err = io.ReadFull(c.br, mask); err != nil {
			return
		}
	}
	payload = make([]byte, length)
	if length > 0 {
		if _, err = io.ReadFull(c.br, payload); err != nil {
			return
		}
	}
	if masked {
		for i := range payload {
			payload[i] ^= mask[i%4]
		}
	} else if op < opClose {
		err = errors.New("客户端数据帧未掩码")
	}
	return
}

// writeFrame 写一帧（服务端帧不掩码；写互斥）。
func (c *wsConn) writeFrame(op byte, payload []byte) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if c.closed {
		return errors.New("connection closed")
	}
	head := []byte{0x80 | op}
	length := len(payload)
	switch {
	case length < 126:
		head = append(head, byte(length))
	case length < 65536:
		head = append(head, 126, 0, 0)
		binary.BigEndian.PutUint16(head[2:], uint16(length))
	default:
		head = append(head, 127, 0, 0, 0, 0, 0, 0, 0, 0)
		binary.BigEndian.PutUint64(head[2:], uint64(length))
	}
	if _, err := c.conn.Write(head); err != nil {
		return err
	}
	if length > 0 {
		if _, err := c.conn.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

// writeCloseFrame 发 close 帧（best-effort）。
func (c *wsConn) writeCloseFrame(code uint16, reason string) {
	body := make([]byte, 2+len(reason))
	binary.BigEndian.PutUint16(body, code)
	copy(body[2:], reason)
	_ = c.writeFrame(opClose, body)
}

// close TCP 层关闭（幂等）。
func (c *wsConn) close() {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if !c.closed {
		c.closed = true
		_ = c.conn.Close()
	}
}
