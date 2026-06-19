package result

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"

	"golang.org/x/net/http2"
	"media-ai-gateway/internal/pipeline"
)

// callbackPayload bọc RecognitionResult với field event_type cho callback body.
// Embedding đảm bảo tất cả field của RecognitionResult được flatten vào JSON.
type callbackPayload struct {
	EventType string `json:"event_type"`
	pipeline.RecognitionResult
}

func eventTypeOf(r pipeline.RecognitionResult) string {
	if r.IsFinal {
		return "asr.transcript.final"
	}
	return "asr.transcript.partial"
}

// HTTPCallbackSink POST RecognitionResult tới một HTTP/2 endpoint.
//
// Transport:
//   - http:// URL → H2C (cleartext HTTP/2, prior-knowledge).
//   - https:// URL → HTTP/2 over TLS (ALPN negotiation).
//
// Retry policy:
//   - HTTP 5xx → retry với constant backoff, tối đa MaxRetry lần.
//   - HTTP 4xx → không retry (lỗi client, không tự phục hồi được).
//   - Network error → retry.
//
// Dead-letter: khi tất cả retry thất bại, result được forward sang DeadLetter sink.
// DeadLetter = nil → drop và trả về lỗi cuối (hành vi cũ).
type HTTPCallbackSink struct {
	URL          string
	MaxRetry     int
	RetryBackoff time.Duration // wait cố định giữa các lần retry; mặc định 200ms
	DeadLetter   Sink          // optional; nhận result khi hết retry
	client       *http.Client
	retries      atomic.Uint64 // tổng số lần retry đã thực hiện
}

// Retries trả về tổng số lần retry đã thực hiện (không tính lần gửi đầu tiên).
func (s *HTTPCallbackSink) Retries() uint64 { return s.retries.Load() }

// NewHTTPCallbackSink tạo HTTPCallbackSink.
// timeout áp dụng cho từng HTTP/2 request; maxRetry là số lần retry (không kể lần đầu).
func NewHTTPCallbackSink(rawURL string, timeout time.Duration, maxRetry int) *HTTPCallbackSink {
	return &HTTPCallbackSink{
		URL:          rawURL,
		MaxRetry:     maxRetry,
		RetryBackoff: 200 * time.Millisecond,
		client: &http.Client{
			Transport: newH2Transport(rawURL),
			Timeout:   timeout,
		},
	}
}

// newH2Transport trả về HTTP/2 transport phù hợp với scheme của URL.
//
//   - http:// → H2C (AllowHTTP + DialTLSContext bypass TLS).
//     http2.Transport.tlsConfig() luôn trả về non-nil *tls.Config nên phải
//     override DialTLSContext để kết nối plain TCP thay vì TLS.
//   - https:// → http2.Transport mặc định, tự negotiate H2 qua ALPN.
func newH2Transport(rawURL string) http.RoundTripper {
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Scheme != "https" {
		return &http2.Transport{
			AllowHTTP: true,
			DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, network, addr)
			},
		}
	}
	return &http2.Transport{}
}

func (s *HTTPCallbackSink) Type() string { return "http_callback" }

// Send POST result tới s.URL với retry. Context-aware: dừng ngay khi ctx done.
// Khi hết retry mà vẫn lỗi, forward sang DeadLetter sink nếu có.
func (s *HTTPCallbackSink) Send(ctx context.Context, r pipeline.RecognitionResult) error {
	body, err := json.Marshal(callbackPayload{
		EventType:           eventTypeOf(r),
		RecognitionResult:   r,
	})
	if err != nil {
		return fmt.Errorf("http_callback: marshal: %w", err)
	}

	var lastErr error
	for attempt := 0; attempt <= s.MaxRetry; attempt++ {
		if attempt > 0 {
			s.retries.Add(1)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(s.RetryBackoff):
			}
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.URL, bytes.NewReader(body))
		if err != nil {
			return fmt.Errorf("http_callback: build request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := s.client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("http_callback: request: %w", err)
			continue // network error → retry
		}
		resp.Body.Close()

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}
		lastErr = fmt.Errorf("http_callback: status %d", resp.StatusCode)
		if resp.StatusCode >= 400 && resp.StatusCode < 500 {
			return lastErr // 4xx: client error, không retry
		}
		// 5xx: server error → retry
	}

	// Hết retry: forward sang dead-letter sink nếu có.
	if s.DeadLetter != nil {
		return s.DeadLetter.Send(ctx, r)
	}
	return lastErr
}
