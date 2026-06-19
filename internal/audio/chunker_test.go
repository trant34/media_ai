package audio

import (
	"encoding/binary"
	"testing"
)

func defaultCfg() ChunkerConfig {
	return ChunkerConfig{SampleRate: 16000, Channels: 1, ChunkMs: 500}
}

// 500ms @ 16kHz = 8000 samples per chunk.

func TestChunker_NoChunkUntilFull(t *testing.T) {
	c := NewChunker(defaultCfg(), "sess", "stream", 0)
	chunks := c.Push(make([]int16, 7999))
	if len(chunks) != 0 {
		t.Errorf("expected 0 chunks, got %d", len(chunks))
	}
	if c.Buffered() != 7999 {
		t.Errorf("Buffered() = %d, want 7999", c.Buffered())
	}
}

func TestChunker_ExactOneChunk(t *testing.T) {
	c := NewChunker(defaultCfg(), "s", "st", 0)
	chunks := c.Push(make([]int16, 8000))
	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(chunks))
	}
	if c.Buffered() != 0 {
		t.Errorf("buffer should be empty, got %d", c.Buffered())
	}
}

func TestChunker_TwoChunksInOnePush(t *testing.T) {
	c := NewChunker(defaultCfg(), "s", "st", 0)
	chunks := c.Push(make([]int16, 16000)) // 1000ms = 2 chunks
	if len(chunks) != 2 {
		t.Fatalf("expected 2 chunks, got %d", len(chunks))
	}
}

func TestChunker_AccumulateAcrossPushes(t *testing.T) {
	c := NewChunker(defaultCfg(), "s", "st", 0)
	c.Push(make([]int16, 4000)) // 250ms — no chunk yet
	chunks := c.Push(make([]int16, 4000)) // total 500ms → 1 chunk
	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk after accumulation, got %d", len(chunks))
	}
}

func TestChunker_Remainder_StaysBuffered(t *testing.T) {
	c := NewChunker(defaultCfg(), "s", "st", 0)
	chunks := c.Push(make([]int16, 9000)) // 8000 chunk + 1000 remain
	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(chunks))
	}
	if c.Buffered() != 1000 {
		t.Errorf("Buffered() = %d, want 1000", c.Buffered())
	}
}

// --- Flush ---

func TestChunker_Flush_Partial(t *testing.T) {
	c := NewChunker(defaultCfg(), "s", "st", 0)
	c.Push(make([]int16, 3000))
	chunk := c.Flush()
	if chunk == nil {
		t.Fatal("Flush() should return partial chunk")
	}
	if len(chunk.PCM) != 3000*2 {
		t.Errorf("PCM len = %d, want %d", len(chunk.PCM), 3000*2)
	}
	if c.Buffered() != 0 {
		t.Error("buffer should be empty after Flush")
	}
}

func TestChunker_Flush_Empty(t *testing.T) {
	c := NewChunker(defaultCfg(), "s", "st", 0)
	if c.Flush() != nil {
		t.Error("Flush() on empty buffer should return nil")
	}
}

func TestChunker_Flush_AfterFullChunk(t *testing.T) {
	c := NewChunker(defaultCfg(), "s", "st", 0)
	c.Push(make([]int16, 8000)) // emits 1 chunk
	c.Push(make([]int16, 2000)) // buffers 2000 remaining
	chunk := c.Flush()
	if chunk == nil {
		t.Fatal("expected partial chunk from Flush")
	}
	if len(chunk.PCM) != 4000 { // 2000 samples * 2 bytes
		t.Errorf("PCM len = %d, want 4000", len(chunk.PCM))
	}
}

// --- PCM S16LE encoding ---

func TestChunker_PCMEncoding(t *testing.T) {
	c := NewChunker(defaultCfg(), "s", "st", 0)
	in := make([]int16, 8000)
	in[0] = 1000
	in[1] = -1
	in[7999] = 32767

	chunks := c.Push(in)
	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk")
	}
	pcm := chunks[0].PCM
	if len(pcm) != 16000 {
		t.Fatalf("PCM len = %d, want 16000", len(pcm))
	}
	if v := int16(binary.LittleEndian.Uint16(pcm[0:])); v != 1000 {
		t.Errorf("pcm[0] = %d, want 1000", v)
	}
	if v := int16(binary.LittleEndian.Uint16(pcm[2:])); v != -1 {
		t.Errorf("pcm[1] = %d, want -1", v)
	}
	if v := int16(binary.LittleEndian.Uint16(pcm[7999*2:])); v != 32767 {
		t.Errorf("pcm[7999] = %d, want 32767", v)
	}
}

func TestChunker_PCMEncoding_MinInt16(t *testing.T) {
	c := NewChunker(defaultCfg(), "s", "st", 0)
	in := make([]int16, 8000)
	in[0] = -32768 // math.MinInt16
	chunks := c.Push(in)
	pcm := chunks[0].PCM
	if v := int16(binary.LittleEndian.Uint16(pcm[0:])); v != -32768 {
		t.Errorf("pcm[0] = %d, want -32768", v)
	}
}

// --- Timestamps ---

func TestChunker_Timestamps_Sequential(t *testing.T) {
	c := NewChunker(defaultCfg(), "s", "st", 1000) // startMs=1000
	chunks := c.Push(make([]int16, 16000))          // 2 chunks
	if len(chunks) != 2 {
		t.Fatalf("expected 2 chunks")
	}
	if chunks[0].TimestampMs != 1000 {
		t.Errorf("chunk[0].TimestampMs = %d, want 1000", chunks[0].TimestampMs)
	}
	if chunks[0].DurationMs != 500 {
		t.Errorf("chunk[0].DurationMs = %d, want 500", chunks[0].DurationMs)
	}
	if chunks[1].TimestampMs != 1500 {
		t.Errorf("chunk[1].TimestampMs = %d, want 1500", chunks[1].TimestampMs)
	}
	if chunks[1].DurationMs != 500 {
		t.Errorf("chunk[1].DurationMs = %d, want 500", chunks[1].DurationMs)
	}
}

func TestChunker_Timestamp_AfterFlush(t *testing.T) {
	c := NewChunker(defaultCfg(), "s", "st", 2000)
	c.Push(make([]int16, 8000)) // emits chunk at 2000ms
	c.Push(make([]int16, 3200)) // 200ms partial (3200/16000*1000 = 200ms)
	chunk := c.Flush()
	if chunk == nil {
		t.Fatal("expected partial chunk from Flush")
	}
	// Timestamp = startMs + 8000 samples * 1000 / 16000 = 2000 + 500 = 2500
	if chunk.TimestampMs != 2500 {
		t.Errorf("Flush chunk.TimestampMs = %d, want 2500", chunk.TimestampMs)
	}
	if chunk.DurationMs != 200 {
		t.Errorf("Flush chunk.DurationMs = %d, want 200", chunk.DurationMs)
	}
}

func TestChunker_Timestamp_Monotonic(t *testing.T) {
	c := NewChunker(defaultCfg(), "s", "st", 0)
	var prev int64 = -1
	// Push 5 chunks worth, one 20ms packet at a time (320 samples each).
	// 5 * 500ms = 25 packets = 8000 samples total.
	for i := 0; i < 25; i++ {
		chunks := c.Push(make([]int16, 320))
		for _, ch := range chunks {
			if ch.TimestampMs <= prev {
				t.Errorf("non-monotonic timestamp: %d after %d", ch.TimestampMs, prev)
			}
			prev = ch.TimestampMs
		}
	}
}

// --- Metadata ---

func TestChunker_Metadata(t *testing.T) {
	c := NewChunker(defaultCfg(), "session-abc", "stream-xyz", 0)
	chunks := c.Push(make([]int16, 8000))
	if len(chunks) != 1 {
		t.Fatal("expected 1 chunk")
	}
	ch := chunks[0]
	if ch.SessionID != "session-abc" {
		t.Errorf("SessionID = %q, want %q", ch.SessionID, "session-abc")
	}
	if ch.StreamID != "stream-xyz" {
		t.Errorf("StreamID = %q, want %q", ch.StreamID, "stream-xyz")
	}
	if ch.SampleRate != 16000 {
		t.Errorf("SampleRate = %d, want 16000", ch.SampleRate)
	}
	if ch.Channels != 1 {
		t.Errorf("Channels = %d, want 1", ch.Channels)
	}
}

// --- Edge cases ---

func TestChunker_Push_Nil(t *testing.T) {
	c := NewChunker(defaultCfg(), "s", "st", 0)
	if c.Push(nil) != nil {
		t.Error("Push(nil) should return nil")
	}
}

func TestChunker_Push_Empty(t *testing.T) {
	c := NewChunker(defaultCfg(), "s", "st", 0)
	if c.Push([]int16{}) != nil {
		t.Error("Push(empty) should return nil")
	}
}

func TestChunker_1000msChunk(t *testing.T) {
	cfg := ChunkerConfig{SampleRate: 16000, Channels: 1, ChunkMs: 1000}
	c := NewChunker(cfg, "s", "st", 0)
	chunks := c.Push(make([]int16, 16000))
	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk for 1000ms, got %d", len(chunks))
	}
	if chunks[0].DurationMs != 1000 {
		t.Errorf("DurationMs = %d, want 1000", chunks[0].DurationMs)
	}
	if len(chunks[0].PCM) != 32000 {
		t.Errorf("PCM len = %d, want 32000", len(chunks[0].PCM))
	}
}
