package ai

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"media-ai-gateway/internal/pipeline"
)

// Sentinel errors trả về bởi Open.
var (
	ErrMaxStreams      = errors.New("ai: max active streams reached")
	ErrDuplicateStream = errors.New("ai: stream already open for session")
)

// Config giữ cấu hình cho AI Stream Manager.
type Config struct {
	MaxActiveStreams int
	QueueSize        int           // per-stream bounded send queue; 0 = direct passthrough (unbounded)
	SendTimeout      time.Duration // timeout per Send() call; 0 = no timeout
	StreamTimeout    time.Duration // hard cap on stream lifetime
	MaxRetries       int           // max reconnect attempts on stream error; 0 = no reconnect
	RetryBackoff     time.Duration // initial backoff between reconnect attempts; 0 → 1s
}

// DefaultConfig trả về cấu hình mặc định phù hợp với production.
func DefaultConfig() Config {
	return Config{
		MaxActiveStreams: 1000,
		QueueSize:        20,
		SendTimeout:      500 * time.Millisecond,
		StreamTimeout:    300 * time.Second,
		MaxRetries:       3,
		RetryBackoff:     time.Second,
	}
}

// ManagerStats là snapshot thống kê tích lũy của Manager (bao gồm cả stream đã đóng).
type ManagerStats struct {
	TotalSendErrors uint64
	TotalRecvErrors uint64
	TotalRetries    uint64
}

// Manager quản lý bidirectional gRPC stream tới AI service, một stream mỗi session.
//
// Goroutine-safe. Stream được tự động xóa khỏi map khi goroutine thoát.
type Manager struct {
	cfg     Config
	dialer  Dialer
	mu      sync.RWMutex
	streams map[string]*Stream // sessionID → Stream

	totalSendErrors atomic.Uint64
	totalRecvErrors atomic.Uint64
	totalRetries    atomic.Uint64
}

// Stats trả về snapshot thống kê tích lũy: tổng lỗi và retries từ cả stream đang chạy và đã đóng.
func (m *Manager) Stats() ManagerStats {
	m.mu.RLock()
	var activeSend, activeRecv uint64
	var activeRetries int
	for _, s := range m.streams {
		st := s.Stats()
		activeSend += st.SendErrors
		activeRecv += st.RecvErrors
		activeRetries += st.Retries
	}
	m.mu.RUnlock()
	return ManagerStats{
		TotalSendErrors: m.totalSendErrors.Load() + activeSend,
		TotalRecvErrors: m.totalRecvErrors.Load() + activeRecv,
		TotalRetries:    m.totalRetries.Load() + uint64(activeRetries),
	}
}

// NewManager tạo Manager với config và dialer cho trước.
func NewManager(cfg Config, dialer Dialer) *Manager {
	return &Manager{
		cfg:     cfg,
		dialer:  dialer,
		streams: make(map[string]*Stream),
	}
}

// Open tạo và khởi động AI stream cho session.
//
//   - parentCtx kiểm soát lifetime: cancel nó (hoặc để StreamTimeout hết hạn) đóng stream.
//   - audioIn  được đọc bởi goroutine send và gửi lên AI worker.
//   - resultOut nhận RecognitionResult từ AI worker.
//
// Stream tự xóa khỏi Manager khi goroutine wrapper thoát.
func (m *Manager) Open(
	parentCtx context.Context,
	sessionID, streamID, language, task string,
	audioIn <-chan pipeline.AudioChunk,
	resultOut chan<- pipeline.RecognitionResult,
) (*Stream, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.streams) >= m.cfg.MaxActiveStreams {
		return nil, ErrMaxStreams
	}
	if _, exists := m.streams[sessionID]; exists {
		return nil, ErrDuplicateStream
	}

	// StreamTimeout là hard cap; parentCtx cancel thường xảy ra trước (session idle 30s).
	ctx, cancel := context.WithTimeout(parentCtx, m.cfg.StreamTimeout)

	// Dial synchronously so Open() returns an error immediately if AI is unreachable.
	client, err := m.dialer.Dial(ctx, sessionID, streamID, language, task)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("ai: dial %s: %w", sessionID, err)
	}

	s := &Stream{
		SessionID: sessionID,
		StreamID:  streamID,
		OpenedAt:  time.Now(),
		cancel:    cancel,
		cfg:       m.cfg,
	}
	s.wg.Add(1)
	go s.runWithReconnect(ctx, m.dialer, client, sessionID, streamID, language, task, audioIn, resultOut)

	m.streams[sessionID] = s

	// Auto-cleanup: accumulate stream stats then remove from map.
	go func() {
		s.wg.Wait()
		st := s.Stats()
		m.totalSendErrors.Add(st.SendErrors)
		m.totalRecvErrors.Add(st.RecvErrors)
		m.totalRetries.Add(uint64(st.Retries))
		m.mu.Lock()
		if m.streams[sessionID] == s {
			delete(m.streams, sessionID)
		}
		m.mu.Unlock()
	}()

	return s, nil
}

// Close đóng stream của session, cancel context của nó.
// Trả về false nếu session không tồn tại.
// Goroutine thoát asynchronously — dùng s.Wait() nếu cần đồng bộ.
func (m *Manager) Close(sessionID string) bool {
	m.mu.Lock()
	s, ok := m.streams[sessionID]
	if ok {
		delete(m.streams, sessionID)
	}
	m.mu.Unlock()
	if !ok {
		return false
	}
	s.cancel()
	return true
}

// Count trả về số stream đang mở.
func (m *Manager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.streams)
}

// MaxStreams trả về giới hạn số stream tối đa (dùng để tính % sử dụng).
func (m *Manager) MaxStreams() int { return m.cfg.MaxActiveStreams }
