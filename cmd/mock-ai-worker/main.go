// Command mock-ai-worker là giả lập AI worker cho môi trường lab/dev.
//
// Implement gRPC service /speech.SpeechStream/Recognize với protobuf codec
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
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/protobuf/encoding/protowire"
)

// ── Protobuf codec ────────────────────────────────────────────────────────────

type protoCodec struct{}

func (protoCodec) Name() string { return "proto" }

func (protoCodec) Marshal(v any) ([]byte, error) {
	m, ok := v.(*recognitionResultWire)
	if !ok {
		return nil, fmt.Errorf("protoCodec: unsupported marshal type %T", v)
	}
	return marshalRecognitionResult(m), nil
}

func (protoCodec) Unmarshal(data []byte, v any) error {
	m, ok := v.(*audioChunkWire)
	if !ok {
		return fmt.Errorf("protoCodec: unsupported unmarshal type %T", v)
	}
	return unmarshalAudioChunk(data, m)
}

func init() { encoding.RegisterCodec(protoCodec{}) }

// ── Wire types ────────────────────────────────────────────────────────────────

type audioChunkWire struct {
	SessionID   string
	StreamID    string
	PCM         []byte
	SampleRate  int
	Channels    int
	RTPTimestamp uint32
	DurationMs  int64
	EndOfStream bool
	Language    string
	Task        string
}

type recognitionResultWire struct {
	SessionID  string
	StreamID   string
	Text       string
	IsFinal    bool
	TsStart    int64
	TsEnd      int64
	Confidence float32
	Language   string
	Seq        uint64
}

// ── Protobuf encode/decode ────────────────────────────────────────────────────

func unmarshalAudioChunk(b []byte, m *audioChunkWire) error {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return protowire.ParseError(n)
		}
		b = b[n:]
		switch {
		case num == 1 && typ == protowire.BytesType:
			v, n := protowire.ConsumeString(b)
			if n < 0 {
				return protowire.ParseError(n)
			}
			m.SessionID = v
			b = b[n:]
		case num == 2 && typ == protowire.BytesType:
			v, n := protowire.ConsumeString(b)
			if n < 0 {
				return protowire.ParseError(n)
			}
			m.StreamID = v
			b = b[n:]
		case num == 3 && typ == protowire.BytesType:
			v, n := protowire.ConsumeBytes(b)
			if n < 0 {
				return protowire.ParseError(n)
			}
			m.PCM = append(m.PCM[:0], v...)
			b = b[n:]
		case num == 4 && typ == protowire.VarintType:
			v, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return protowire.ParseError(n)
			}
			m.SampleRate = int(int32(v))
			b = b[n:]
		case num == 5 && typ == protowire.VarintType:
			v, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return protowire.ParseError(n)
			}
			m.Channels = int(int32(v))
			b = b[n:]
		case num == 6 && typ == protowire.VarintType:
			v, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return protowire.ParseError(n)
			}
			m.RTPTimestamp = uint32(v)
			b = b[n:]
		case num == 7 && typ == protowire.VarintType:
			v, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return protowire.ParseError(n)
			}
			m.DurationMs = int64(v)
			b = b[n:]
		case num == 8 && typ == protowire.VarintType:
			v, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return protowire.ParseError(n)
			}
			m.EndOfStream = v != 0
			b = b[n:]
		case num == 9 && typ == protowire.BytesType:
			v, n := protowire.ConsumeString(b)
			if n < 0 {
				return protowire.ParseError(n)
			}
			m.Language = v
			b = b[n:]
		case num == 10 && typ == protowire.BytesType:
			v, n := protowire.ConsumeString(b)
			if n < 0 {
				return protowire.ParseError(n)
			}
			m.Task = v
			b = b[n:]
		default:
			n := protowire.ConsumeFieldValue(num, typ, b)
			if n < 0 {
				return protowire.ParseError(n)
			}
			b = b[n:]
		}
	}
	return nil
}

func marshalRecognitionResult(m *recognitionResultWire) []byte {
	var b []byte
	if m.SessionID != "" {
		b = protowire.AppendTag(b, 1, protowire.BytesType)
		b = protowire.AppendString(b, m.SessionID)
	}
	if m.StreamID != "" {
		b = protowire.AppendTag(b, 2, protowire.BytesType)
		b = protowire.AppendString(b, m.StreamID)
	}
	if m.Text != "" {
		b = protowire.AppendTag(b, 3, protowire.BytesType)
		b = protowire.AppendString(b, m.Text)
	}
	if m.IsFinal {
		b = protowire.AppendTag(b, 4, protowire.VarintType)
		b = protowire.AppendVarint(b, 1)
	}
	if m.TsStart != 0 {
		b = protowire.AppendTag(b, 5, protowire.VarintType)
		b = protowire.AppendVarint(b, uint64(m.TsStart))
	}
	if m.TsEnd != 0 {
		b = protowire.AppendTag(b, 6, protowire.VarintType)
		b = protowire.AppendVarint(b, uint64(m.TsEnd))
	}
	if m.Confidence != 0 {
		b = protowire.AppendTag(b, 7, protowire.Fixed32Type)
		b = protowire.AppendFixed32(b, math.Float32bits(m.Confidence))
	}
	if m.Language != "" {
		b = protowire.AppendTag(b, 8, protowire.BytesType)
		b = protowire.AppendString(b, m.Language)
	}
	if m.Seq != 0 {
		b = protowire.AppendTag(b, 9, protowire.VarintType)
		b = protowire.AppendVarint(b, m.Seq)
	}
	return b
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
		chunkCount   uint64
		seq          uint64
		sessionID    string
		streamID     string
		language     string
		startRTPTs   uint32
	)

	for {
		var chunk audioChunkWire
		if err := stream.RecvMsg(&chunk); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		if chunkCount == 0 {
			sessionID = chunk.SessionID
			streamID = chunk.StreamID
			language = chunk.Language
			if language == "" {
				language = "vi"
			}
			startRTPTs = chunk.RTPTimestamp
			slog.Info("stream opened",
				"session_id", sessionID,
				"stream_id", streamID,
				"language", language,
				"task", chunk.Task,
			)
		}
		chunkCount++

		if chunkCount%3 == 0 {
			seq++
			partial := &recognitionResultWire{
				SessionID:  sessionID,
				StreamID:   streamID,
				Text:       partialPhrases[(chunkCount/3-1)%uint64(len(partialPhrases))],
				IsFinal:    false,
				TsStart:    int64(startRTPTs),
				TsEnd:      int64(chunk.RTPTimestamp),
				Confidence: 0,
				Language:   language,
				Seq:        seq,
			}
			if err := stream.SendMsg(partial); err != nil {
				return err
			}
			slog.Debug("sent partial", "session_id", sessionID, "seq", seq, "text", partial.Text)
		}

		if chunkCount%6 == 0 {
			seq++
			final := &recognitionResultWire{
				SessionID:  sessionID,
				StreamID:   streamID,
				Text:       finalPhrases[(chunkCount/6-1)%uint64(len(finalPhrases))],
				IsFinal:    true,
				TsStart:    int64(startRTPTs),
				TsEnd:      int64(chunk.RTPTimestamp),
				Confidence: 0.92,
				Language:   language,
				Seq:        seq,
			}
			if err := stream.SendMsg(final); err != nil {
				return err
			}
			slog.Info("sent final",
				"session_id", sessionID,
				"seq", seq,
				"text", final.Text,
				"chunks", chunkCount,
			)
			startRTPTs = chunk.RTPTimestamp
		}

		if chunk.EndOfStream {
			seq++
			final := &recognitionResultWire{
				SessionID:  sessionID,
				StreamID:   streamID,
				Text:       fmt.Sprintf("[end] %s", finalPhrases[seq%uint64(len(finalPhrases))]),
				IsFinal:    true,
				TsStart:    int64(startRTPTs),
				TsEnd:      int64(chunk.RTPTimestamp),
				Confidence: 0.95,
				Language:   language,
				Seq:        seq,
			}
			if err := stream.SendMsg(final); err != nil {
				return err
			}
			slog.Info("stream closed (end_of_stream)",
				"session_id", sessionID,
				"total_chunks", chunkCount,
				"total_seq", seq,
			)
			return nil
		}
	}

	slog.Info("stream closed (client EOF)",
		"session_id", sessionID,
		"total_chunks", chunkCount,
		"total_seq", seq,
	)
	return nil
}

// ── Main ──────────────────────────────────────────────────────────────────────

func main() {
	addr := flag.String("addr", envOr("MOCK_AI_ADDR", ":50051"), "gRPC listen address")
	logLevel := flag.String("log-level", envOr("MOCK_AI_LOG", "info"), "debug|info|warn|error")
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

	srv := grpc.NewServer(
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             20 * time.Second, // gateway mặc định ping mỗi 30s — cho phép xuống 20s
			PermitWithoutStream: true,             // cho phép ping khi không có stream active
		}),
	)
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
