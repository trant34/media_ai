// Package ai quản lý gRPC stream tới AI service theo từng session.
package ai

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"media-ai-gateway/internal/pipeline"
)

// StreamClient abstracts một bidirectional gRPC stream với AI service.
type StreamClient interface {
	Send(chunk pipeline.AudioChunk) error
	Recv() (pipeline.RecognitionResult, error)
	CloseSend() error
}

// Dialer mở một bidirectional stream mới tới AI service.
type Dialer interface {
	Dial(ctx context.Context, sessionID, streamID, language, task string) (StreamClient, error)
}

// Stream quản lý goroutine send và recv cho một AI gRPC stream.
type Stream struct {
	SessionID string
	StreamID  string
	OpenedAt  time.Time

	cancel context.CancelFunc
	wg     sync.WaitGroup
	cfg    Config

	mu      sync.Mutex
	err     error
	retries int

	sendErrors atomic.Uint64
	recvErrors atomic.Uint64
}

// StreamStats là snapshot counter của một Stream.
type StreamStats struct {
	SendErrors uint64
	RecvErrors uint64
	Retries    int
}

// Stats trả về snapshot thống kê của stream tại thời điểm gọi.
func (s *Stream) Stats() StreamStats {
	s.mu.Lock()
	retries := s.retries
	s.mu.Unlock()
	return StreamStats{
		SendErrors: s.sendErrors.Load(),
		RecvErrors: s.recvErrors.Load(),
		Retries:    retries,
	}
}

// Err trả về lỗi đầu tiên không phải io.EOF từ send hoặc recv.
func (s *Stream) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

func (s *Stream) setErr(err error) {
	s.mu.Lock()
	if s.err == nil {
		s.err = err
	}
	s.mu.Unlock()
}

func (s *Stream) clearErr() {
	s.mu.Lock()
	s.err = nil
	s.mu.Unlock()
}

// Retries trả về số lần reconnect đã thực hiện.
func (s *Stream) Retries() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.retries
}

// Wait block cho đến khi wrapper goroutine đã thoát.
func (s *Stream) Wait() { s.wg.Wait() }

// runWithReconnect là wrapper goroutine duy nhất. Quản lý reconnect theo cfg.MaxRetries.
// Initial client đã được dial đồng bộ trong Open() — truyền thẳng vào đây.
func (s *Stream) runWithReconnect(
	ctx context.Context,
	dialer Dialer,
	client StreamClient,
	sessionID, streamID, language, task string,
	audioIn <-chan pipeline.AudioChunk,
	resultOut chan<- pipeline.RecognitionResult,
) {
	defer s.wg.Done()

	backoff := s.cfg.RetryBackoff
	if backoff == 0 {
		backoff = time.Second
	}

	for {
		s.runPair(ctx, client, audioIn, resultOut)

		// Nếu ctx bị cancel từ ngoài, thoát ngay — không reconnect.
		if ctx.Err() != nil {
			return
		}

		pairErr := s.Err()
		if pairErr == nil || s.cfg.MaxRetries == 0 {
			return
		}

		s.mu.Lock()
		if s.retries >= s.cfg.MaxRetries {
			s.mu.Unlock()
			return
		}
		s.retries++
		s.mu.Unlock()

		s.clearErr()

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}

		backoff *= 2
		if backoff > 30*time.Second {
			backoff = 30 * time.Second
		}

		newClient, err := dialer.Dial(ctx, sessionID, streamID, language, task)
		if err != nil {
			s.setErr(err)
			return
		}
		client = newClient
	}
}

// runPair chạy một cặp send+recv cho một connection attempt.
// Thoát khi send hoặc recv exit; gọi client.CloseSend() khi kết thúc.
func (s *Stream) runPair(
	ctx context.Context,
	client StreamClient,
	audioIn <-chan pipeline.AudioChunk,
	resultOut chan<- pipeline.RecognitionResult,
) {
	pairCtx, pairCancel := context.WithCancel(ctx)
	defer pairCancel()

	var wg sync.WaitGroup

	var sendInput <-chan pipeline.AudioChunk
	if s.cfg.QueueSize > 0 {
		q := make(chan pipeline.AudioChunk, s.cfg.QueueSize)
		sendInput = q
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer close(q)
			for {
				select {
				case <-pairCtx.Done():
					return
				case chunk, ok := <-audioIn:
					if !ok {
						return
					}
					select {
					case q <- chunk:
					default: // drop khi queue đầy
					}
				}
			}
		}()
	} else {
		sendInput = audioIn
	}

	wg.Add(2)
	go func() {
		defer wg.Done()
		defer pairCancel()
		s.runSend(pairCtx, client, sendInput)
		// CloseSend signals the recv side to unblock when send exits first.
		client.CloseSend()
	}()
	go func() {
		defer wg.Done()
		defer pairCancel()
		s.runRecv(pairCtx, client, resultOut)
	}()

	wg.Wait()
}

// sendWithTimeout wraps client.Send với timeout nếu cfg.SendTimeout > 0.
// Trả về context.DeadlineExceeded nếu hết timeout.
func (s *Stream) sendWithTimeout(ctx context.Context, client StreamClient, chunk pipeline.AudioChunk) error {
	if s.cfg.SendTimeout == 0 {
		return client.Send(chunk)
	}
	type result struct{ err error }
	ch := make(chan result, 1)
	go func() {
		ch <- result{client.Send(chunk)}
	}()
	select {
	case r := <-ch:
		return r.err
	case <-time.After(s.cfg.SendTimeout):
		return context.DeadlineExceeded
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Stream) runSend(ctx context.Context, client StreamClient, audioIn <-chan pipeline.AudioChunk) {
	for {
		select {
		case <-ctx.Done():
			return
		case chunk, ok := <-audioIn:
			if !ok {
				return
			}
			if err := s.sendWithTimeout(ctx, client, chunk); err != nil {
				if !isCtxErr(err) {
					s.sendErrors.Add(1)
					s.setErr(err)
				}
				return
			}
		}
	}
}

// runRecv gọi client.Recv() trong vòng lặp và dispatch kết quả sang resultOut.
//
// Backpressure policy:
//   - Partial result (IsFinal=false) bị drop nếu resultOut đầy.
//   - Final result (IsFinal=true) block cho đến khi được deliver (hoặc ctx done).
func (s *Stream) runRecv(ctx context.Context, client StreamClient, resultOut chan<- pipeline.RecognitionResult) {
	for {
		r, err := client.Recv()
		if err != nil {
			if err != io.EOF {
				s.recvErrors.Add(1)
				s.setErr(err)
			}
			return
		}
		select {
		case resultOut <- r:
			continue
		case <-ctx.Done():
			return
		default:
		}
		if !r.IsFinal {
			continue
		}
		select {
		case resultOut <- r:
		case <-ctx.Done():
			return
		}
	}
}

func isCtxErr(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
