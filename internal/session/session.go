// Package session quản lý lifecycle của media session.
package session

import (
	"context"
	"sync"
	"time"

	"media-ai-gateway/internal/pipeline"
)

// Status là trạng thái vòng đời của một session.
type Status string

const (
	StatusCreated Status = "CREATED"
	StatusActive  Status = "ACTIVE"
	StatusIdle    Status = "IDLE"
	StatusClosing Status = "CLOSING"
	StatusClosed  Status = "CLOSED"
)

// Session giữ toàn bộ state của một media processing session.
//
// Ctx bị cancel khi session đóng. Mọi consumer (pipeline worker, AI stream, result dispatcher)
// nên select trên Ctx.Done() để dừng đọc từ các queue.
//
// Các channel (PacketQueue, AudioQueue, ResultQueue) KHÔNG bị close khi session đóng —
// chúng được GC tự động khi mọi tham chiếu mất đi. Điều này tránh panic khi
// producer vẫn còn đang ghi vào channel tại thời điểm close.
type Session struct {
	ID          string
	SourceType  string // "raw_rtp" | "webrtc"
	SSRC        uint32
	PayloadType uint8
	Codec       string
	SampleRate  int
	Channels    int
	Language    string
	Task        string
	CallbackURL    string
	PeerID         string
	TrackID        string
	RemoteAddr     string                   // expected source UDP addr "ip:port"; used as fallback routing key
	MediaResources *pipeline.MediaResources // H.248 termination info set via PATCH

	PacketQueue chan pipeline.MediaPacket
	AudioQueue  chan pipeline.AudioChunk
	ResultQueue chan pipeline.RecognitionResult

	GatewayID string // ID of the gateway node that created this session
	CreatedAt time.Time

	// Ctx bị cancel khi session Close(). Consumer select trên Ctx.Done() để thoát.
	Ctx    context.Context
	cancel context.CancelFunc

	mu           sync.Mutex
	lastPacketAt time.Time
	status       Status
}

// Touch cập nhật lastPacketAt và chuyển sang Active nếu đang ở Created/Idle.
func (s *Session) Touch() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastPacketAt = time.Now()
	if s.status == StatusCreated || s.status == StatusIdle {
		s.status = StatusActive
	}
}

// LastPacketAt trả về thời điểm nhận packet gần nhất.
func (s *Session) LastPacketAt() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastPacketAt
}

// Status trả về trạng thái hiện tại của session.
func (s *Session) Status() Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.status
}

func (s *Session) setStatus(st Status) {
	s.mu.Lock()
	s.status = st
	s.mu.Unlock()
}

// isIdle trả về true nếu session không nhận packet trong khoảng idleTimeout.
// Session đang CLOSING hoặc CLOSED không được coi là idle.
func (s *Session) isIdle(now time.Time, idleTimeout time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.status == StatusClosed || s.status == StatusClosing {
		return false
	}
	return now.Sub(s.lastPacketAt) > idleTimeout
}
