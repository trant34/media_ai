package pipeline

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"media-ai-gateway/internal/audio"
	"media-ai-gateway/internal/codec"
)

// SessionConfig mô tả audio pipeline cần tạo cho một session.
type SessionConfig struct {
	// Input từ codec
	Codec      string // "PCMU", "PCMA", "opus"
	SampleRate int    // input sample rate; 0 = dùng default của codec
	Channels   int    // input channels; 0 = dùng default của codec

	// Output về chunker
	OutSampleRate int // Hz đầu ra (ví dụ: 16000)
	OutChannels   int // kênh đầu ra (thường 1)
	ChunkMs       int // độ dài mỗi AudioChunk (ms)

	// Metadata
	SessionID string
	StreamID  string
	StartMs   int64 // wall-clock ms của packet đầu tiên, gốc timestamp cho chunks

	// Debug: nếu non-empty, ghi raw decoded PCM (int16-LE) vào thư mục này.
	// Tên file: <session_id>.<codec>.<rate>hz.<ch>ch.s16le
	// Phát lại: ffplay -f s16le -ar <rate> -ac <ch> <file>
	// Empty = tắt.
	PCMDumpDir string
}

// sessionPipeline giữ state xử lý âm thanh theo từng session.
// Mutex bảo vệ Resampler và Chunker khỏi data race khi nhiều worker cạnh tranh
// cùng một session (Resampler có phase accumulator, Chunker có PCM buffer).
type sessionPipeline struct {
	mu        sync.Mutex
	decoder   codec.Decoder
	resampler *audio.Resampler
	chunker   *audio.Chunker
	audioOut  chan<- AudioChunk
	ctx       context.Context // session ctx: dừng emit khi session đóng
	pcmFile   *os.File        // non-nil khi PCM dump được bật
	pcmBuf    *bufio.Writer   // bufio wrapper để giảm syscall per-packet
	pcmPath   string          // full path để log khi close
	pcmBytes  int64           // tổng byte đã ghi
}

// process decode + resample + chunk một MediaPacket.
// Trả về số chunks đã emit và decode error (nếu có).
func (sp *sessionPipeline) process(pkt MediaPacket) (int, error) {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	if sp.ctx.Err() != nil {
		return 0, nil // session đã đóng
	}

	pcm, err := sp.decoder.Decode(pkt.Payload)
	if err != nil {
		return 0, fmt.Errorf("pipeline: decode: %w", err)
	}
	if len(pcm) == 0 {
		return 0, nil
	}

	if sp.pcmBuf != nil {
		if err := binary.Write(sp.pcmBuf, binary.LittleEndian, pcm); err != nil {
			slog.Warn("pcm dump write error", "path", sp.pcmPath, "err", err)
			sp.pcmBuf = nil // tắt dump sau lỗi để tránh lặp log
		} else {
			sp.pcmBytes += int64(len(pcm) * 2)
		}
	}

	resampled := sp.resampler.Resample(pcm)
	chunks := sp.chunker.Push(resampled)

	emitted := 0
	for _, ac := range chunks {
		chunk := AudioChunk{
			SessionID:   ac.SessionID,
			StreamID:    ac.StreamID,
			PCM:         ac.PCM,
			SampleRate:  ac.SampleRate,
			Channels:    ac.Channels,
			TimestampMs: ac.TimestampMs,
			DurationMs:  ac.DurationMs,
		}
		select {
		case sp.audioOut <- chunk:
			emitted++
		case <-sp.ctx.Done():
			return emitted, nil
		}
	}
	return emitted, nil
}

// flush phát phần PCM còn lại trong Chunker buffer như partial chunk.
// Gọi khi session đóng để không mất dữ liệu.
func (sp *sessionPipeline) flush() {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	if sp.pcmFile != nil {
		if sp.pcmBuf != nil {
			if err := sp.pcmBuf.Flush(); err != nil {
				slog.Warn("pcm dump flush error", "path", sp.pcmPath, "err", err)
			}
			sp.pcmBuf = nil
		}
		_ = sp.pcmFile.Sync()
		sp.pcmFile.Close()
		sp.pcmFile = nil
		slog.Debug("pcm dump closed", "path", sp.pcmPath, "bytes", sp.pcmBytes)
	}

	ac := sp.chunker.Flush()
	if ac == nil {
		return
	}
	select {
	case sp.audioOut <- AudioChunk{
		SessionID:   ac.SessionID,
		StreamID:    ac.StreamID,
		PCM:         ac.PCM,
		SampleRate:  ac.SampleRate,
		Channels:    ac.Channels,
		TimestampMs: ac.TimestampMs,
		DurationMs:  ac.DurationMs,
		EndOfStream: true,
	}:
	default:
	}
}

// newSessionPipeline tạo sessionPipeline từ SessionConfig.
func newSessionPipeline(ctx context.Context, cfg SessionConfig, audioOut chan<- AudioChunk) (*sessionPipeline, error) {
	dec, err := codec.New(cfg.Codec, cfg.SampleRate, cfg.Channels)
	if err != nil {
		return nil, fmt.Errorf("pipeline: codec: %w", err)
	}
	resampler := audio.NewResampler(dec.SampleRate(), dec.Channels(), cfg.OutSampleRate)
	chunker := audio.NewChunker(
		audio.ChunkerConfig{
			SampleRate: cfg.OutSampleRate,
			Channels:   cfg.OutChannels,
			ChunkMs:    cfg.ChunkMs,
		},
		cfg.SessionID, cfg.StreamID, cfg.StartMs,
	)

	var pcmFile *os.File
	var pcmBuf *bufio.Writer
	var pcmPath string
	if cfg.PCMDumpDir != "" {
		if mkErr := os.MkdirAll(cfg.PCMDumpDir, 0o755); mkErr != nil {
			return nil, fmt.Errorf("pipeline: pcm dump dir: %w", mkErr)
		}
		pcmPath = filepath.Join(cfg.PCMDumpDir, fmt.Sprintf("%s.%s.%dhz.%dch.s16le",
			cfg.SessionID,
			strings.ToLower(strings.ReplaceAll(cfg.Codec, "-", "")),
			dec.SampleRate(),
			dec.Channels(),
		))
		pcmFile, err = os.OpenFile(pcmPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			return nil, fmt.Errorf("pipeline: pcm dump open: %w", err)
		}
		pcmBuf = bufio.NewWriterSize(pcmFile, 32*1024) // 32 KiB buffer ≈ 100 packets
		slog.Info("pcm dump opened", "path", pcmPath, "session_id", cfg.SessionID)
	}

	return &sessionPipeline{
		decoder:   dec,
		resampler: resampler,
		chunker:   chunker,
		audioOut:  audioOut,
		ctx:       ctx,
		pcmFile:   pcmFile,
		pcmBuf:    pcmBuf,
		pcmPath:   pcmPath,
	}, nil
}
