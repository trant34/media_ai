package pipeline

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

var (
	ErrQueueFull       = errors.New("pipeline: submit queue full")
	ErrSessionExists   = errors.New("pipeline: session already registered")
	ErrSessionNotFound = errors.New("pipeline: session not found")
)

// WorkerPoolConfig cấu hình WorkerPool.
type WorkerPoolConfig struct {
	Workers   int // số goroutine worker song song
	QueueSize int // giới hạn job queue
}

// DefaultWorkerPoolConfig trả về cấu hình production phù hợp.
func DefaultWorkerPoolConfig() WorkerPoolConfig {
	return WorkerPoolConfig{Workers: 16, QueueSize: 8192}
}

// Stats giữ thống kê của WorkerPool.
type Stats struct {
	Submitted    uint64 // tổng Submit() calls
	Dropped      uint64 // jobs bị drop (queue đầy)
	Processed    uint64 // jobs được xử lý thành công
	DecodeErrors uint64 // lỗi decode (payload không hợp lệ)
}

// WorkerPool nhận AudioJob từ nhiều goroutine, xử lý qua pipeline per-session.
//
// Luồng: Submit() → bounded queue → worker pool → sessionPipeline → audioOut channel.
//
// Submit không bao giờ block: nếu queue đầy, job bị drop và trả ErrQueueFull.
// Goroutine-safe. Run() phải được gọi để khởi động workers.
type WorkerPool struct {
	cfg   WorkerPoolConfig
	queue chan AudioJob

	mu        sync.RWMutex
	pipelines map[string]*sessionPipeline

	submitted    atomic.Uint64
	dropped      atomic.Uint64
	processed    atomic.Uint64
	decodeErrors atomic.Uint64
}

// NewWorkerPool tạo WorkerPool với config cho trước.
func NewWorkerPool(cfg WorkerPoolConfig) *WorkerPool {
	return &WorkerPool{
		cfg:       cfg,
		queue:     make(chan AudioJob, cfg.QueueSize),
		pipelines: make(map[string]*sessionPipeline),
	}
}

// RegisterSession tạo pipeline cho session. audioOut nhận AudioChunk đã xử lý.
// Trả về ErrSessionExists nếu session đã được đăng ký.
func (wp *WorkerPool) RegisterSession(ctx context.Context, cfg SessionConfig, audioOut chan<- AudioChunk) error {
	sp, err := newSessionPipeline(ctx, cfg, audioOut)
	if err != nil {
		return err
	}

	wp.mu.Lock()
	defer wp.mu.Unlock()

	if _, ok := wp.pipelines[cfg.SessionID]; ok {
		return ErrSessionExists
	}
	wp.pipelines[cfg.SessionID] = sp
	return nil
}

// UnregisterSession xóa pipeline của session và flush partial chunk còn lại.
// Trả về false nếu session không tồn tại.
func (wp *WorkerPool) UnregisterSession(sessionID string) bool {
	wp.mu.Lock()
	sp, ok := wp.pipelines[sessionID]
	if ok {
		delete(wp.pipelines, sessionID)
	}
	wp.mu.Unlock()

	if !ok {
		return false
	}
	sp.flush()
	return true
}

// Submit đẩy AudioJob vào queue (non-blocking).
// Trả về ErrQueueFull nếu queue đầy; không bao giờ block.
func (wp *WorkerPool) Submit(ctx context.Context, job AudioJob) error {
	wp.submitted.Add(1)

	select {
	case wp.queue <- job:
		return nil
	case <-ctx.Done():
		wp.dropped.Add(1)
		return ctx.Err()
	default:
		wp.dropped.Add(1)
		return ErrQueueFull
	}
}

// Run khởi động worker pool và block cho đến khi ctx done.
func (wp *WorkerPool) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for i := 0; i < wp.cfg.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			wp.worker(ctx)
		}()
	}
	wg.Wait()
}

func (wp *WorkerPool) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-wp.queue:
			wp.mu.RLock()
			sp, ok := wp.pipelines[job.SessionID]
			wp.mu.RUnlock()

			if !ok {
				continue
			}

			_, decErr := sp.process(job.Packet)
			if decErr != nil {
				wp.decodeErrors.Add(1)
			} else {
				wp.processed.Add(1)
			}
		}
	}
}

// Stats trả về bản sao thống kê tại thời điểm gọi.
func (wp *WorkerPool) Stats() Stats {
	return Stats{
		Submitted:    wp.submitted.Load(),
		Dropped:      wp.dropped.Load(),
		Processed:    wp.processed.Load(),
		DecodeErrors: wp.decodeErrors.Load(),
	}
}

// SessionCount trả về số session đang đăng ký.
func (wp *WorkerPool) SessionCount() int {
	wp.mu.RLock()
	defer wp.mu.RUnlock()
	return len(wp.pipelines)
}

// QueueLen trả về số job đang chờ trong queue.
func (wp *WorkerPool) QueueLen() int { return len(wp.queue) }

// QueueCap trả về tổng dung lượng của job queue (dùng để tính % sử dụng).
func (wp *WorkerPool) QueueCap() int { return wp.cfg.QueueSize }
