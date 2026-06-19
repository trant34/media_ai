package ai

import (
	"context"
	"encoding/json"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"

	"media-ai-gateway/internal/pipeline"
)

// recognizeRPC là full gRPC method path phía client cần gọi.
// AI worker phải expose đúng path này.
const recognizeRPC = "/speech.SpeechStream/Recognize"

// jsonCodec là gRPC codec dùng JSON thay cho protobuf.
// Cho phép tránh bước code generation proto — AI worker phải tương thích.
// Content-Type trên wire: "application/grpc+json".
type jsonCodec struct{}

func (jsonCodec) Marshal(v any) ([]byte, error)      { return json.Marshal(v) }
func (jsonCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }
func (jsonCodec) Name() string                        { return "json" }

func init() { encoding.RegisterCodec(jsonCodec{}) }

// audioChunkWire là wire format JSON của AudioChunk.
// Bổ sung Language và Task (không có trong pipeline.AudioChunk nhưng cần gửi AI).
type audioChunkWire struct {
	SessionID   string `json:"session_id"`
	StreamID    string `json:"stream_id"`
	PCM         []byte `json:"pcm"`
	SampleRate  int    `json:"sample_rate"`
	Channels    int    `json:"channels"`
	TimestampMs int64  `json:"timestamp_ms"`
	DurationMs  int64  `json:"duration_ms"`
	Language    string `json:"language,omitempty"`
	Task        string `json:"task,omitempty"`
}

// recognitionResultWire là wire format JSON của RecognitionResult.
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

// NewGRPCDialFunc trả về GRPCDialFunc mở bidirectional gRPC stream thực sự
// đến AI worker tại workerAddr (insecure, không TLS).
//
// Yêu cầu phía AI worker:
//   - Expose gRPC tại workerAddr.
//   - Implement /speech.SpeechStream/Recognize (bidirectional streaming).
//   - Chấp nhận JSON-encoded AudioChunk, trả JSON-encoded RecognitionResult.
//   - Xem internal/ai/proto/speech.proto cho contract đầy đủ.
func NewGRPCDialFunc() GRPCDialFunc {
	return func(ctx context.Context, workerAddr, _, _, language, task string) (StreamClient, error) {
		conn, err := grpc.NewClient(workerAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return nil, fmt.Errorf("grpc: connect %s: %w", workerAddr, err)
		}

		desc := &grpc.StreamDesc{
			StreamName:    "Recognize",
			ClientStreams: true,
			ServerStreams: true,
		}
		cs, err := conn.NewStream(ctx, desc, recognizeRPC, grpc.ForceCodec(jsonCodec{}))
		if err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("grpc: open stream to %s: %w", workerAddr, err)
		}
		return &grpcStreamClient{conn: conn, cs: cs, language: language, task: task}, nil
	}
}

// grpcStreamClient wraps gRPC ClientStream thành StreamClient interface.
// Giữ language/task để gắn vào mỗi AudioChunk gửi đi (pipeline.AudioChunk không có 2 field này).
type grpcStreamClient struct {
	conn     *grpc.ClientConn
	cs       grpc.ClientStream
	language string
	task     string
}

func (c *grpcStreamClient) Send(chunk pipeline.AudioChunk) error {
	return c.cs.SendMsg(&audioChunkWire{
		SessionID:   chunk.SessionID,
		StreamID:    chunk.StreamID,
		PCM:         chunk.PCM,
		SampleRate:  chunk.SampleRate,
		Channels:    chunk.Channels,
		TimestampMs: chunk.TimestampMs,
		DurationMs:  chunk.DurationMs,
		Language:    c.language,
		Task:        c.task,
	})
}

func (c *grpcStreamClient) Recv() (pipeline.RecognitionResult, error) {
	var w recognitionResultWire
	if err := c.cs.RecvMsg(&w); err != nil {
		return pipeline.RecognitionResult{}, err
	}
	return pipeline.RecognitionResult{
		SessionID:  w.SessionID,
		StreamID:   w.StreamID,
		Text:       w.Text,
		IsFinal:    w.IsFinal,
		StartMs:    w.StartMs,
		EndMs:      w.EndMs,
		Confidence: w.Confidence,
		Language:   w.Language,
		Seq:        w.Seq,
	}, nil
}

// CloseSend signals end-of-stream lên server và đóng kết nối.
// Mỗi stream dùng một ClientConn riêng nên đóng là an toàn.
func (c *grpcStreamClient) CloseSend() error {
	err := c.cs.CloseSend()
	_ = c.conn.Close()
	return err
}
