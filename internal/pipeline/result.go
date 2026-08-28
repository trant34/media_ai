package pipeline

// MediaResource identifies an H.248/MEGACO media termination.
type MediaResource struct {
	ContextID     string `json:"contextId,omitempty"`
	TerminationID string `json:"terminationId,omitempty"`
}

// MediaResources holds the core-side and access-side H.248 terminations for a session.
// Each session only populates one side (TCore or TAccess); omitempty ensures the absent
// side does not serialize as {"contextId":"","terminationId":""} in the callback payload.
type MediaResources struct {
	TCore   *MediaResource `json:"tCore,omitempty"`
	TAccess *MediaResource `json:"tAccess,omitempty"`
}

// RecognitionResult là transcript/kết quả trả về từ AI Worker.
// Nếu AudioPayload != nil, coordinator sẽ gửi payload này về MF qua RTP egress
// thay vì (hoặc song song với) dispatch text sang callback.
type RecognitionResult struct {
	SessionID      string          `json:"session_id"`
	StreamID       string          `json:"stream_id"`
	SourceType     string          `json:"source_type"`
	Text           string          `json:"text"`
	IsFinal        bool            `json:"is_final"`
	TsStart        int64           `json:"ts_start"`
	TsEnd          int64           `json:"ts_end"`
	Confidence     float32         `json:"confidence,omitempty"`
	Language       string          `json:"language,omitempty"`
	Seq            uint64          `json:"seq"`
	MediaResources *MediaResources `json:"mediaResources,omitempty"`

	// AudioPayload chứa audio đã encode (cùng codec với session, e.g. PCMU)
	// do AI Worker trả về để gateway gửi ngược lại MF qua RTP.
	// Nil = text-only result (hành vi hiện tại).
	AudioPayload []byte `json:"audio_payload,omitempty"`
	// AudioPT là RTP payload type của AudioPayload (e.g. 0 cho PCMU).
	AudioPT uint8 `json:"audio_pt,omitempty"`
}
