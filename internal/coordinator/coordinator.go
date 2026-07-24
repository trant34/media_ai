// Package coordinator wire toàn bộ per-session pipeline:
// PacketQueue → Jitter Buffer → Worker Pool → AI Stream → Result Dispatcher.
//
// Mỗi lần Start(sess) khởi động tất cả goroutine cho session đó.
// Goroutine tự thoát khi sess.Ctx bị cancel.
package coordinator

import (
	"log/slog"
	"sync"
	"time"

	"media-ai-gateway/internal/ai"
	"media-ai-gateway/internal/jitter"
	"media-ai-gateway/internal/pipeline"
	"media-ai-gateway/internal/result"
	"media-ai-gateway/internal/session"
)

// Config cấu hình pipeline mỗi session.
type Config struct {
	JitterConfig    jitter.Config
	OutSampleRate   int           // Hz sau resample (ví dụ: 16000)
	OutChannels     int           // kênh đầu ra (thường 1 = mono)
	ChunkMs         int           // độ dài AudioChunk gửi AI (ms)
	CallbackTimeout time.Duration // timeout mỗi HTTP callback request (dùng khi CallbackClient = nil)
	CallbackRetry   int           // số lần retry HTTP callback (không kể lần đầu)
	PCMDumpDir      string        // nếu non-empty, ghi decoded PCM vào thư mục này (debug)
	CallbackClient  *result.CallbackHTTPClient // shared H/2 client; nil = per-sink transport (backward compat)
}

// DefaultConfig trả về cấu hình phù hợp cho production ASR.
func DefaultConfig() Config {
	return Config{
		JitterConfig:    jitter.DefaultConfig(),
		OutSampleRate:   16000,
		OutChannels:     1,
		ChunkMs:         500,
		CallbackTimeout: 5 * time.Second,
		CallbackRetry:   3,
	}
}

// Coordinator wire các component lại với nhau cho mỗi session.
// Goroutine-safe: Start có thể gọi đồng thời cho nhiều session.
type Coordinator struct {
	cfg        Config
	pool       *pipeline.WorkerPool
	aiMgr      *ai.Manager
	dispatcher *result.Dispatcher

	mu         sync.RWMutex
	jitterBufs map[string]*jitter.Buffer // sessionID → active jitter buffer
}

// New tạo Coordinator với config và dependency cho trước.
func New(cfg Config, pool *pipeline.WorkerPool, aiMgr *ai.Manager, dispatcher *result.Dispatcher) *Coordinator {
	return &Coordinator{
		cfg:        cfg,
		pool:       pool,
		aiMgr:      aiMgr,
		dispatcher: dispatcher,
		jitterBufs: make(map[string]*jitter.Buffer),
	}
}

// Start kích hoạt per-session pipeline:
//  1. Đăng ký sink HTTP callback (nếu CallbackURL được cấu hình)
//  2. Đăng ký session với WorkerPool (decode → resample → chunk → AudioQueue)
//  3. Mở AI stream (audioIn=AudioQueue, resultOut=ResultQueue)
//  4. Jitter pump: PacketQueue → jitter buffer → WorkerPool.Submit
//  5. Result pump: ResultQueue → Dispatcher.Push
//  6. Cleanup goroutine: sau sess.Ctx.Done → UnregisterSession + UnregisterSinks
//
// Rollback: nếu bước 2 hoặc 3 thất bại, mọi đăng ký đã thực hiện đều được hoàn tác.
// Khi Start trả về nil, toàn bộ goroutine đã chạy và pipeline sẵn sàng nhận packet.
// HTTPCallbackSink được trả về (hoặc nil) để caller đăng ký vào metrics server.
func (c *Coordinator) Start(sess *session.Session) (*result.HTTPCallbackSink, error) {
	streamID := sess.ID
	if sess.TrackID != "" {
		streamID = sess.TrackID
	}

	// 1. HTTP callback sink.
	var cbSink *result.HTTPCallbackSink
	if sess.CallbackURL != "" {
		cbSink = c.newCallbackSink(sess.CallbackURL)
		c.dispatcher.RegisterSink(sess.ID, cbSink)
	}

	// 2. Worker pool registration.
	poolCfg := pipeline.SessionConfig{
		Codec:         sess.Codec,
		SampleRate:    sess.SampleRate,
		Channels:      sess.Channels,
		OutSampleRate: c.cfg.OutSampleRate,
		OutChannels:   c.cfg.OutChannels,
		ChunkMs:       c.cfg.ChunkMs,
		SessionID:     sess.ID,
		StreamID:      streamID,
		PCMDumpDir:    c.cfg.PCMDumpDir,
	}
	if err := c.pool.RegisterSession(sess.Ctx, poolCfg, sess.AudioQueue); err != nil {
		c.dispatcher.UnregisterSinks(sess.ID)
		return nil, err
	}

	// 3. AI stream — rollback pool on failure.
	if _, err := c.aiMgr.Open(
		sess.Ctx, sess.ID, streamID, sess.Language, sess.Task,
		sess.AudioQueue, sess.ResultQueue,
	); err != nil {
		c.pool.UnregisterSession(sess.ID)
		c.dispatcher.UnregisterSinks(sess.ID)
		return nil, err
	}

	// 4. Jitter pump goroutines.
	c.startJitterPump(sess)

	// 5. Result pump goroutine.
	go c.resultPump(sess)

	// 6. Cleanup sau khi session đóng.
	go func() {
		<-sess.Ctx.Done()
		c.pool.UnregisterSession(sess.ID)
		c.dispatcher.UnregisterSinks(sess.ID)
		c.mu.Lock()
		delete(c.jitterBufs, sess.ID)
		c.mu.Unlock()
	}()

	return cbSink, nil
}

// startJitterPump khởi động 3 goroutine per-session:
//   - jitter.Buffer.Run(ctx, jitterOut): flush loop mỗi PacketTimeMs
//   - packet pump: PacketQueue → buf.Push + sess.Touch()
//   - submit pump: jitterOut → pool.Submit (non-blocking, drop nếu pool queue đầy)
func (c *Coordinator) startJitterPump(sess *session.Session) {
	jitterOut := make(chan pipeline.MediaPacket, 16)
	buf := jitter.New(c.cfg.JitterConfig)

	c.mu.Lock()
	c.jitterBufs[sess.ID] = buf
	c.mu.Unlock()

	go buf.Run(sess.Ctx, jitterOut)

	go func() {
		for {
			select {
			case <-sess.Ctx.Done():
				return
			case pkt := <-sess.PacketQueue:
				buf.Push(pkt)
				sess.Touch()
			}
		}
	}()

	go func() {
		for {
			select {
			case <-sess.Ctx.Done():
				return
			case pkt := <-jitterOut:
				job := pipeline.AudioJob{SessionID: sess.ID, Packet: pkt}
				_ = c.pool.Submit(sess.Ctx, job)
			}
		}
	}()
}

func (c *Coordinator) resultPump(sess *session.Session) {
	streamID := sess.ID
	if sess.TrackID != "" {
		streamID = sess.TrackID
	}
	for {
		select {
		case <-sess.Ctx.Done():
			return
		case r := <-sess.ResultQueue:
			if r.SessionID == "" {
				r.SessionID = sess.ID
			}
			if r.StreamID == "" {
				r.StreamID = streamID
			}
			if r.SourceType == "" {
				r.SourceType = sess.SourceType
			}
			if r.MediaResources == nil && sess.MediaResources != nil {
				r.MediaResources = sess.MediaResources
			}
			slog.Debug("ai: result received",
				"session_id", r.SessionID,
				"stream_id", r.StreamID,
				"is_final", r.IsFinal,
				"text", r.Text,
				"seq", r.Seq,
				"confidence", r.Confidence,
				"language", r.Language,
				"ts_start", r.TsStart,
				"ts_end", r.TsEnd,
			)
			_ = c.dispatcher.Push(sess.Ctx, r)
		}
	}
}

// UpdateCallbackSink thay thế HTTP callback sink của session.
// Nếu newURL rỗng, sink hiện tại được giữ nguyên (không override bằng nil).
// Trả về sink mới (hoặc nil nếu newURL rỗng) để caller đăng ký vào metrics.
func (c *Coordinator) UpdateCallbackSink(sessID, newURL string) *result.HTTPCallbackSink {
	if newURL == "" {
		return nil
	}
	cbSink := c.newCallbackSink(newURL)
	c.dispatcher.ReplaceHTTPSink(sessID, cbSink)
	return cbSink
}

// newCallbackSink tạo HTTPCallbackSink dùng shared client nếu có, hoặc per-sink transport.
func (c *Coordinator) newCallbackSink(rawURL string) *result.HTTPCallbackSink {
	if c.cfg.CallbackClient != nil {
		return c.cfg.CallbackClient.NewHTTPCallbackSink(rawURL, c.cfg.CallbackRetry)
	}
	return result.NewHTTPCallbackSink(rawURL, c.cfg.CallbackTimeout, c.cfg.CallbackRetry)
}

// AggregateJitterStats trả về tổng hợp jitter stats từ tất cả session đang active.
func (c *Coordinator) AggregateJitterStats() (received, released, dropped, lost uint64) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, buf := range c.jitterBufs {
		st := buf.Stats()
		received += st.Received
		released += st.Released
		dropped += st.Dropped
		lost += st.Lost
	}
	return
}
