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
type RecognitionResult struct {
	SessionID      string          `json:"session_id"`
	StreamID       string          `json:"stream_id"`
	SourceType     string          `json:"source_type"`
	Text           string          `json:"text"`
	IsFinal        bool            `json:"is_final"`
	StartMs        int64           `json:"start_ms"`
	EndMs          int64           `json:"end_ms"`
	Confidence     float32         `json:"confidence,omitempty"`
	Language       string          `json:"language,omitempty"`
	Seq            uint64          `json:"seq"`
	MediaResources *MediaResources `json:"mediaResources,omitempty"`
}
