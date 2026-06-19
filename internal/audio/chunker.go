// Package audio xử lý PCM: resample, mix stereo→mono, chunk.
package audio

// Chunk là một đoạn PCM có độ dài cố định được tạo bởi Chunker.
// pipeline.AudioChunk được tạo từ Chunk này (tách để tránh import cycle).
type Chunk struct {
	SessionID   string
	StreamID    string
	PCM         []byte
	SampleRate  int
	Channels    int
	TimestampMs int64
	DurationMs  int64
}

// ChunkerConfig định nghĩa tham số chunk đầu ra.
type ChunkerConfig struct {
	SampleRate int // Hz đầu ra (ví dụ: 16000)
	Channels   int // kênh đầu ra (1 = mono)
	ChunkMs    int // độ dài chunk tính bằng ms (ví dụ: 500)
}

// Chunker tích lũy PCM int16 frames và phát ra AudioChunk có độ dài cố định.
//
// Timestamp được tính từ số sample đã phát, không phải wall clock,
// nên chính xác dù có jitter xử lý.
//
// Không goroutine-safe: dùng một Chunker per session/goroutine.
type Chunker struct {
	cfg       ChunkerConfig
	sessionID string
	streamID  string
	startMs   int64

	buf        []int16 // accumulation buffer
	samplesOut int64   // tổng mono samples đã phát
	chunkSamps int     // samples/chunk = SampleRate * ChunkMs / 1000
}

// NewChunker tạo Chunker. startMs là thời điểm wall-clock của sample đầu tiên,
// dùng làm gốc cho timestamp của mọi chunk.
func NewChunker(cfg ChunkerConfig, sessionID, streamID string, startMs int64) *Chunker {
	if cfg.Channels <= 0 {
		cfg.Channels = 1
	}
	chunkSamps := cfg.SampleRate * cfg.ChunkMs / 1000
	return &Chunker{
		cfg:        cfg,
		sessionID:  sessionID,
		streamID:   streamID,
		startMs:    startMs,
		buf:        make([]int16, 0, chunkSamps*2),
		chunkSamps: chunkSamps,
	}
}

// Buffered trả về số PCM samples đang giữ trong buffer.
func (c *Chunker) Buffered() int { return len(c.buf) }

// Push nối pcm vào buffer và trả về các chunk hoàn chỉnh (nếu có).
// Trả về nil nếu chưa đủ data.
func (c *Chunker) Push(pcm []int16) []Chunk {
	if len(pcm) == 0 {
		return nil
	}
	c.buf = append(c.buf, pcm...)

	if len(c.buf) < c.chunkSamps {
		return nil
	}

	var chunks []Chunk
	for len(c.buf) >= c.chunkSamps {
		chunks = append(chunks, c.emit(c.buf[:c.chunkSamps]))
		c.buf = c.buf[c.chunkSamps:]
	}
	return chunks
}

// Flush phát phần còn lại trong buffer như một partial chunk.
// Trả về nil nếu buffer rỗng.
func (c *Chunker) Flush() *Chunk {
	if len(c.buf) == 0 {
		return nil
	}
	chunk := c.emit(c.buf)
	c.buf = c.buf[:0]
	return &chunk
}

// emit tạo Chunk từ samples và cập nhật bộ đếm sample.
func (c *Chunker) emit(samples []int16) Chunk {
	tsMs := c.startMs + c.samplesOut*1000/int64(c.cfg.SampleRate)
	durMs := int64(len(samples)) * 1000 / int64(c.cfg.SampleRate)
	chunk := Chunk{
		SessionID:   c.sessionID,
		StreamID:    c.streamID,
		PCM:         Int16ToS16LE(samples),
		SampleRate:  c.cfg.SampleRate,
		Channels:    c.cfg.Channels,
		TimestampMs: tsMs,
		DurationMs:  durMs,
	}
	c.samplesOut += int64(len(samples))
	return chunk
}
