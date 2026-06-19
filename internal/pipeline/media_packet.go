package pipeline

// MediaPacket là model chuẩn hóa từ Raw RTP và WebRTC ingress.
type MediaPacket struct {
	SessionID   string
	SourceType  string // "raw_rtp" | "webrtc"

	SSRC        uint32
	PayloadType uint8
	Sequence    uint16
	Timestamp   uint32
	Marker      bool

	Payload      []byte
	ReceivedAtMs int64

	Codec      string
	SampleRate int
	Channels   int
}

// AudioChunk là output của audio pipeline, sẵn sàng gửi sang AI.
type AudioChunk struct {
	SessionID   string
	StreamID    string
	PCM         []byte
	SampleRate  int
	Channels    int
	TimestampMs int64
	DurationMs  int64
}
