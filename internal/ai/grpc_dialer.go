package ai

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/protobuf/encoding/protowire"

	"media-ai-gateway/internal/pipeline"
)

// recognizeRPC là full gRPC method path phía client cần gọi.
const recognizeRPC = "/speech.SpeechStream/Recognize"

// protoCodec encode AudioChunk và decode RecognitionResult theo protobuf binary.
// Content-Type trên wire: "application/grpc+proto" (chuẩn gRPC).
type protoCodec struct{}

func (protoCodec) Name() string { return "proto" }

func (protoCodec) Marshal(v any) ([]byte, error) {
	m, ok := v.(*audioChunkWire)
	if !ok {
		return nil, fmt.Errorf("protoCodec: unsupported marshal type %T", v)
	}
	return marshalAudioChunk(m), nil
}

func (protoCodec) Unmarshal(data []byte, v any) error {
	m, ok := v.(*recognitionResultWire)
	if !ok {
		return fmt.Errorf("protoCodec: unsupported unmarshal type %T", v)
	}
	return unmarshalRecognitionResult(data, m)
}

// marshalAudioChunk encode AudioChunk theo protobuf binary (field numbers từ speech.proto).
func marshalAudioChunk(m *audioChunkWire) []byte {
	var b []byte
	if m.SessionID != "" {
		b = protowire.AppendTag(b, 1, protowire.BytesType)
		b = protowire.AppendString(b, m.SessionID)
	}
	if m.StreamID != "" {
		b = protowire.AppendTag(b, 2, protowire.BytesType)
		b = protowire.AppendString(b, m.StreamID)
	}
	if len(m.PCM) > 0 {
		b = protowire.AppendTag(b, 3, protowire.BytesType)
		b = protowire.AppendBytes(b, m.PCM)
	}
	if m.SampleRate != 0 {
		b = protowire.AppendTag(b, 4, protowire.VarintType)
		b = protowire.AppendVarint(b, uint64(int32(m.SampleRate)))
	}
	if m.Channels != 0 {
		b = protowire.AppendTag(b, 5, protowire.VarintType)
		b = protowire.AppendVarint(b, uint64(int32(m.Channels)))
	}
	if m.TimestampMs != 0 {
		b = protowire.AppendTag(b, 6, protowire.VarintType)
		b = protowire.AppendVarint(b, uint64(m.TimestampMs))
	}
	if m.DurationMs != 0 {
		b = protowire.AppendTag(b, 7, protowire.VarintType)
		b = protowire.AppendVarint(b, uint64(m.DurationMs))
	}
	if m.EndOfStream {
		b = protowire.AppendTag(b, 8, protowire.VarintType)
		b = protowire.AppendVarint(b, 1)
	}
	if m.Language != "" {
		b = protowire.AppendTag(b, 9, protowire.BytesType)
		b = protowire.AppendString(b, m.Language)
	}
	if m.Task != "" {
		b = protowire.AppendTag(b, 10, protowire.BytesType)
		b = protowire.AppendString(b, m.Task)
	}
	return b
}

// unmarshalRecognitionResult decode protobuf binary → RecognitionResult.
func unmarshalRecognitionResult(b []byte, m *recognitionResultWire) error {
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
			v, n := protowire.ConsumeString(b)
			if n < 0 {
				return protowire.ParseError(n)
			}
			m.Text = v
			b = b[n:]
		case num == 4 && typ == protowire.VarintType:
			v, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return protowire.ParseError(n)
			}
			m.IsFinal = v != 0
			b = b[n:]
		case num == 5 && typ == protowire.VarintType:
			v, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return protowire.ParseError(n)
			}
			m.StartMs = int64(v)
			b = b[n:]
		case num == 6 && typ == protowire.VarintType:
			v, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return protowire.ParseError(n)
			}
			m.EndMs = int64(v)
			b = b[n:]
		case num == 7 && typ == protowire.Fixed32Type:
			v, n := protowire.ConsumeFixed32(b)
			if n < 0 {
				return protowire.ParseError(n)
			}
			m.Confidence = math.Float32frombits(v)
			b = b[n:]
		case num == 8 && typ == protowire.BytesType:
			v, n := protowire.ConsumeString(b)
			if n < 0 {
				return protowire.ParseError(n)
			}
			m.Language = v
			b = b[n:]
		case num == 9 && typ == protowire.VarintType:
			v, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return protowire.ParseError(n)
			}
			m.Seq = v
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

// audioChunkWire là wire type của AudioChunk gửi tới AI worker.
// Language và Task không có trong pipeline.AudioChunk nên được truyền riêng.
type audioChunkWire struct {
	SessionID   string
	StreamID    string
	PCM         []byte
	SampleRate  int
	Channels    int
	TimestampMs int64
	DurationMs  int64
	EndOfStream bool
	Language    string
	Task        string
}

// recognitionResultWire là wire type nhận từ AI worker.
type recognitionResultWire struct {
	SessionID  string
	StreamID   string
	Text       string
	IsFinal    bool
	StartMs    int64
	EndMs      int64
	Confidence float32
	Language   string
	Seq        uint64
}

// SharedConnPool giữ một *grpc.ClientConn dùng chung per AI worker address.
// Tất cả session tới cùng địa chỉ tái dùng 1 kết nối HTTP/2 duy nhất — mỗi session
// chỉ mở thêm 1 grpc.ClientStream mới trên connection đó (HTTP/2 multiplexing).
//
// Thread-safe. Conn được tạo lazy khi địa chỉ đầu tiên được dial.
type SharedConnPool struct {
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
	opts  []grpc.DialOption
}

// NewSharedConnPool tạo pool với keepalive params.
//   - keepaliveTime: gửi HTTP/2 PING sau khoảng idle này (0 = không PING).
//   - keepaliveTimeout: đóng conn nếu không nhận PONG sau khoảng này.
func NewSharedConnPool(keepaliveTime, keepaliveTimeout time.Duration) *SharedConnPool {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	if keepaliveTime > 0 {
		opts = append(opts, grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                keepaliveTime,
			Timeout:             keepaliveTimeout,
			PermitWithoutStream: true, // gửi PING ngay cả khi không có stream active
		}))
	}
	return &SharedConnPool{
		conns: make(map[string]*grpc.ClientConn),
		opts:  opts,
	}
}

// getOrCreate trả về conn hiện có hoặc tạo conn mới cho addr.
func (p *SharedConnPool) getOrCreate(addr string) (*grpc.ClientConn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if conn, ok := p.conns[addr]; ok {
		return conn, nil
	}
	conn, err := grpc.NewClient(addr, p.opts...)
	if err != nil {
		return nil, err
	}
	p.conns[addr] = conn
	return conn, nil
}

// Preconnect khởi tạo và trigger async TCP dial tới addr ngay khi app start.
// Non-fatal: chỉ log nếu lỗi — gateway vẫn hoạt động, gRPC retry khi có session đầu tiên.
func (p *SharedConnPool) Preconnect(addr string) {
	conn, err := p.getOrCreate(addr)
	if err != nil {
		slog.Warn("ai: grpc preconnect failed", "addr", addr, "err", err)
		return
	}
	conn.Connect() // trigger async dial; gRPC tự retry nếu server chưa sẵn sàng
	slog.Info("ai: gRPC connection initiated", "addr", addr, "state", conn.GetState())
}

// State trả về trạng thái kết nối tới addr (dùng để health check / logging).
func (p *SharedConnPool) State(addr string) connectivity.State {
	p.mu.Lock()
	conn, ok := p.conns[addr]
	p.mu.Unlock()
	if !ok {
		return connectivity.Idle
	}
	return conn.GetState()
}

// Addrs trả về danh sách địa chỉ đã có conn trong pool.
func (p *SharedConnPool) Addrs() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	addrs := make([]string, 0, len(p.conns))
	for addr := range p.conns {
		addrs = append(addrs, addr)
	}
	return addrs
}

// DialFunc trả về GRPCDialFunc mở grpc.ClientStream mới trên shared conn.
// Wire encoding: protobuf binary (Content-Type: application/grpc+proto).
func (p *SharedConnPool) DialFunc() GRPCDialFunc {
	return func(ctx context.Context, workerAddr, _, _, language, task string) (StreamClient, error) {
		conn, err := p.getOrCreate(workerAddr)
		if err != nil {
			return nil, fmt.Errorf("grpc: connect %s: %w", workerAddr, err)
		}
		desc := &grpc.StreamDesc{
			StreamName:    "Recognize",
			ClientStreams: true,
			ServerStreams: true,
		}
		cs, err := conn.NewStream(ctx, desc, recognizeRPC, grpc.ForceCodec(protoCodec{}))
		if err != nil {
			return nil, fmt.Errorf("grpc: open stream to %s: %w", workerAddr, err)
		}
		return &grpcStreamClient{cs: cs, language: language, task: task}, nil
	}
}

// grpcStreamClient wraps gRPC ClientStream thành StreamClient interface.
// conn được quản lý bởi SharedConnPool — không đóng ở đây.
type grpcStreamClient struct {
	cs        grpc.ClientStream
	language  string
	task      string
	firstSent atomic.Bool
}

func (c *grpcStreamClient) Send(chunk pipeline.AudioChunk) error {
	if c.firstSent.CompareAndSwap(false, true) {
		slog.Debug("ai: first chunk sent to worker",
			"session_id", chunk.SessionID,
			"stream_id", chunk.StreamID,
			"language", c.language,
			"task", c.task,
			"pcm_bytes", len(chunk.PCM),
			"sample_rate", chunk.SampleRate,
			"end_of_stream", chunk.EndOfStream,
		)
	}
	return c.cs.SendMsg(&audioChunkWire{
		SessionID:   chunk.SessionID,
		StreamID:    chunk.StreamID,
		PCM:         chunk.PCM,
		SampleRate:  chunk.SampleRate,
		Channels:    chunk.Channels,
		TimestampMs: chunk.TimestampMs,
		DurationMs:  chunk.DurationMs,
		EndOfStream: chunk.EndOfStream,
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

// CloseSend gửi gRPC half-close (END_STREAM) lên server.
func (c *grpcStreamClient) CloseSend() error {
	return c.cs.CloseSend()
}

// Close là no-op: conn được giữ bởi SharedConnPool và tái dùng qua các session.
func (c *grpcStreamClient) Close() error { return nil }
