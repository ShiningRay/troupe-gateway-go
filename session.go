package gateway

import (
	"net/http"
	"sync"
	"time"
)

// wsFrame 出站帧（mailbox 载荷）：opcode + 载荷。
type wsFrame struct {
	op   byte
	data []byte
}

// Session 一条外部连接在网关内的表示。纯数据 + 出站 mailbox（容量/溢出策略由 Gateway 执行）。
type Session struct {
	ID      string
	Meta    map[string]string
	Path    string
	Query   map[string]string
	Headers http.Header

	mailbox chan wsFrame
	socket  *wsConn

	mu            sync.Mutex
	cleaned       bool
	mailboxClosed bool
}

func newSession(id string, mailboxCap int, path string, query map[string]string, headers http.Header) *Session {
	return &Session{
		ID:      id,
		Meta:    map[string]string{},
		Path:    path,
		Query:   query,
		Headers: headers,
		mailbox: make(chan wsFrame, mailboxCap),
	}
}

// IsCleaned 清理标记（断线路径幂等）。
func (s *Session) IsCleaned() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cleaned
}

func (s *Session) markCleaned() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	was := s.cleaned
	s.cleaned = true
	return !was
}

// ClosedMailbox mailbox 是否已关闭。
func (s *Session) ClosedMailbox() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mailboxClosed
}

// mailboxSend 限时入队（持锁防与 close 竞态，避免 send-on-closed panic）。
func (s *Session) mailboxSend(f wsFrame, wait <-chan time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mailboxClosed {
		return false
	}
	select {
	case s.mailbox <- f:
		return true
	case <-wait:
		return false
	}
}

// mailboxTrySend 非阻塞入队（控制帧用：满/关即丢）。
func (s *Session) mailboxTrySend(f wsFrame) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mailboxClosed {
		return false
	}
	select {
	case s.mailbox <- f:
		return true
	default:
		return false
	}
}

// mailboxClose 关闭 mailbox（写循环排空后退出）。
func (s *Session) mailboxClose() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.mailboxClosed {
		s.mailboxClosed = true
		close(s.mailbox)
	}
}
