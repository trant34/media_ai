package controlplane

import "time"

// CreateSessionRequest là JSON body cho POST /v1/sessions.
type CreateSessionRequest struct {
	ID          string `json:"id"`
	SourceType  string `json:"source_type"`        // "raw_rtp" | "webrtc"
	SSRC        uint32 `json:"ssrc,omitempty"`
	PayloadType uint8  `json:"payload_type,omitempty"`
	Codec       string `json:"codec"`              // "PCMU" | "PCMA" | "opus"
	SampleRate  int    `json:"sample_rate"`
	Channels    int    `json:"channels,omitempty"` // default 1
	Language    string `json:"language,omitempty"`
	Task        string `json:"task,omitempty"`
	CallbackURL string `json:"callback_url,omitempty"`
	// RemoteAddr là địa chỉ UDP nguồn "ip:port" của caller.
	// Dùng làm fallback routing khi SSRC=0 trong shared ingress mode.
	RemoteAddr  string `json:"remote_addr,omitempty"`
}

// SessionResponse là JSON response cho session endpoints.
// POST /v1/sessions populates GatewayID, RTPIP, RTPPort when applicable;
// GET /v1/sessions/{id} omits those fields.
type SessionResponse struct {
	SessionID  string    `json:"session_id"`
	Status     string    `json:"status"`
	SourceType string    `json:"source_type"`
	Codec      string    `json:"codec"`
	SampleRate int       `json:"sample_rate"`
	Channels   int       `json:"channels"`
	Language   string    `json:"language,omitempty"`
	Task       string    `json:"task,omitempty"`
	CreatedAt  time.Time `json:"created_at"`

	// Populated on CREATE for raw_rtp sessions.
	GatewayID string `json:"gateway_id,omitempty"`
	RTPIP     string `json:"rtp_ip,omitempty"`
	RTPPort   int    `json:"rtp_port,omitempty"`
}

// StatsResponse là JSON response cho GET /v1/stats.
type StatsResponse struct {
	Sessions   int             `json:"sessions"`
	AIStreams   int             `json:"ai_streams"`
	Pool       PoolStats       `json:"pool"`
	Dispatcher DispatcherStats `json:"dispatcher"`
	AI         AIStats         `json:"ai"`
}

// AIStats là thống kê tích lũy của AI Stream Manager.
type AIStats struct {
	TotalSendErrors uint64 `json:"total_send_errors"`
	TotalRecvErrors uint64 `json:"total_recv_errors"`
	TotalRetries    uint64 `json:"total_retries"`
}

// PoolStats là stats của AudioProc Worker Pool.
type PoolStats struct {
	Submitted      uint64 `json:"submitted"`
	Dropped        uint64 `json:"dropped"`
	Processed      uint64 `json:"processed"`
	DecodeErrors   uint64 `json:"decode_errors"`
	ActiveSessions int    `json:"active_sessions"`
	QueueLen       int    `json:"queue_len"`
}

// DispatcherStats là stats của Result Dispatcher.
type DispatcherStats struct {
	Pushed      uint64 `json:"pushed"`
	Dropped     uint64 `json:"dropped"`
	Sent        uint64 `json:"sent"`
	SentPartial uint64 `json:"sent_partial"`
	SentFinal   uint64 `json:"sent_final"`
	SendErrors  uint64 `json:"send_errors"`
	QueueLen    int    `json:"queue_len"`
}

// ErrorResponse là JSON body khi có lỗi.
type ErrorResponse struct {
	Error        string `json:"error"`
	RetryAfterMs int64  `json:"retry_after_ms,omitempty"`
}
