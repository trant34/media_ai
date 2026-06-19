package pipeline

// RecognitionResult là transcript/kết quả trả về từ AI Worker.
type RecognitionResult struct {
	SessionID  string  `json:"session_id"`
	StreamID   string  `json:"stream_id"`
	SourceType string  `json:"source_type"`
	Text       string  `json:"text"`
	IsFinal    bool    `json:"is_final"`
	StartMs    int64   `json:"start_ms"`
	EndMs      int64   `json:"end_ms"`
	Confidence float32 `json:"confidence,omitempty"`
	Language   string  `json:"language,omitempty"`
	Seq        uint64  `json:"seq"`
}
