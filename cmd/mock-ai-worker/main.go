// Command mock-ai-worker là giả lập AI worker cho môi trường lab/dev.
//
// Implement gRPC service /speech.SpeechStream/Recognize với JSON codec
// (tương thích gateway). Với mỗi session:
//   - Mỗi 3 AudioChunk nhận được → gửi 1 partial result
//   - Mỗi 6 AudioChunk nhận được → gửi 1 final result
//   - Khi end_of_stream=true     → gửi final result và đóng stream
//
// Usage:
//
//	mock-ai-worker [--addr :50051]
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
)

// ── JSON codec (phải khớp với grpc_dialer.go của gateway) ────────────────────

type jsonCodec struct{}

func (jsonCodec) Marshal(v any) ([]byte, error)      { return json.Marshal(v) }
func (jsonCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }
func (jsonCodec) Name() string                        { return "json" }

func init() { encoding.RegisterCodec(jsonCodec{}) }

// ── Wire types (phải khớp với grpc_dialer.go) ─────────────────────────────────

type audioChunkWire struct {
	SessionID    string `json:"session_id"`
	StreamID     string `json:"stream_id"`
	PCM          []byte `json:"pcm"`
	SampleRate   int    `json:"sample_rate"`
	Channels     int    `json:"channels"`
	TimestampMs  int64  `json:"timestamp_ms"`
	DurationMs   int64  `json:"duration_ms"`
	Language     string `json:"language,omitempty"`
	Task         string `json:"task,omitempty"`
	EndOfStream  bool   `json:"end_of_stream,omitempty"`
}

type recognitionResultWire struct {
	SessionID  string  `json:"session_id"`
	StreamID   string  `json:"stream_id"`
	Text       string  `json:"text"`
	IsFinal    bool    `json:"is_final"`
	StartMs    int64   `json:"start_ms"`
	EndMs      int64   `json:"end_ms"`
	Confidence float32 `json:"confidence,omitempty"`
	Language   string  `json:"language,omitempty"`
	Seq        uint64  `json:"seq"`
}

// ── Mock phrases ──────────────────────────────────────────────────────────────

var partialPhrases = []string{
	"xin chào...",
	"đây là kết quả...",
	"đang nhận dạng giọng nói...",
	"âm thanh đang được xử lý...",
}

var finalPhrases = []string{
	"Xin chào, tôi cần hỗ trợ.",
	"Cuộc gọi đến từ khách hàng.",
	"Vui lòng chờ trong giây lát.",
	"Cảm ơn bạn đã liên hệ.",
	"Tôi muốn biết thêm thông tin.",
}

// ── gRPC handler ──────────────────────────────────────────────────────────────

func recognizeHandler(_ any, stream grpc.ServerStream) error {
	var (
		chunkCount uint64
		seq        uint64
		sessionID  string
		streamID   string
		language   string
		startMs    int64
	)

	startMs = time.Now().UnixMilli()

	for {
		var chunk audioChunkWire
		if err := stream.RecvMsg(&chunk); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		// Lấy metadata từ chunk đầu tiên
		if chunkCount == 0 {
			sessionID = chunk.SessionID
			streamID  = chunk.StreamID
			language  = chunk.Language
			if language == "" {
				language = "vi"
			}
			startMs = chunk.TimestampMs
			slog.Info("stream opened",
				"session_id", sessionID,
				"stream_id",  streamID,
				"language",   language,
				"task",       chunk.Task,
			)
		}
		chunkCount++

		endMs := time.Now().UnixMilli()

		// Partial result mỗi 3 chunk
		if chunkCount%3 == 0 {
			seq++
			partial := &recognitionResultWire{
				SessionID:  sessionID,
				StreamID:   streamID,
				Text:       partialPhrases[(chunkCount/3-1)%uint64(len(partialPhrases))],
				IsFinal:    false,
				StartMs:    startMs,
				EndMs:      endMs,
				Confidence: 0,
				Language:   language,
				Seq:        seq,
			}
			if err := stream.SendMsg(partial); err != nil {
				return err
			}
			slog.Debug("sent partial", "session_id", sessionID, "seq", seq, "text", partial.Text)
		}

		// Final result mỗi 6 chunk
		if chunkCount%6 == 0 {
			seq++
			final := &recognitionResultWire{
				SessionID:  sessionID,
				StreamID:   streamID,
				Text:       finalPhrases[(chunkCount/6-1)%uint64(len(finalPhrases))],
				IsFinal:    true,
				StartMs:    startMs,
				EndMs:      endMs,
				Confidence: 0.92,
				Language:   language,
				Seq:        seq,
			}
			if err := stream.SendMsg(final); err != nil {
				return err
			}
			slog.Info("sent final",
				"session_id", sessionID,
				"seq",        seq,
				"text",       final.Text,
				"chunks",     chunkCount,
			)
			startMs = endMs // reset window
		}

		// end_of_stream: gửi final cuối cùng và đóng
		if chunk.EndOfStream {
			seq++
			final := &recognitionResultWire{
				SessionID:  sessionID,
				StreamID:   streamID,
				Text:       fmt.Sprintf("[end] %s", finalPhrases[seq%uint64(len(finalPhrases))]),
				IsFinal:    true,
				StartMs:    startMs,
				EndMs:      time.Now().UnixMilli(),
				Confidence: 0.95,
				Language:   language,
				Seq:        seq,
			}
			if err := stream.SendMsg(final); err != nil {
				return err
			}
			slog.Info("stream closed (end_of_stream)",
				"session_id",  sessionID,
				"total_chunks", chunkCount,
				"total_seq",   seq,
			)
			return nil
		}
	}

	slog.Info("stream closed (client EOF)",
		"session_id",   sessionID,
		"total_chunks", chunkCount,
		"total_seq",    seq,
	)
	return nil
}

// ── Main ──────────────────────────────────────────────────────────────────────

func main() {
	addr     := flag.String("addr",      envOr("MOCK_AI_ADDR", ":50051"), "gRPC listen address")
	logLevel := flag.String("log-level", envOr("MOCK_AI_LOG",  "info"),   "debug|info|warn|error")
	flag.Parse()

	var l slog.Level
	if err := l.UnmarshalText([]byte(*logLevel)); err != nil {
		l = slog.LevelInfo
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: l})))

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		slog.Error("listen failed", "addr", *addr, "err", err)
		os.Exit(1)
	}

	srv := grpc.NewServer()
	srv.RegisterService(&grpc.ServiceDesc{
		ServiceName: "speech.SpeechStream",
		HandlerType: (*any)(nil),
		Methods:     []grpc.MethodDesc{},
		Streams: []grpc.StreamDesc{
			{
				StreamName:    "Recognize",
				Handler:       recognizeHandler,
				ServerStreams: true,
				ClientStreams: true,
			},
		},
	}, struct{}{})

	slog.Info("mock-ai-worker listening", "addr", *addr)
	if err := srv.Serve(ln); err != nil {
		slog.Error("serve error", "err", err)
		os.Exit(1)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
