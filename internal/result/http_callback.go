package result

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/http2"
	"media-ai-gateway/internal/pipeline"
)

// dualH2Transport route request sang h2c transport (http://) hoặc TLS transport (https://).
// Cho phép 1 shared http.Client xử lý cả 2 scheme mà không cần tạo riêng per-sink.
type dualH2Transport struct {
	h2c   *http2.Transport // AllowHTTP + plain TCP dial
	h2tls *http2.Transport // standard TLS via ALPN
}

func (d *dualH2Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Scheme == "https" {
		return d.h2tls.RoundTrip(req)
	}
	return d.h2c.RoundTrip(req)
}

// CallbackHTTPClient là shared HTTP/2 client dùng chung toàn bộ gateway.
// http2.Transport pool connection per host — tất cả session cùng callback server
// tự động reuse 1 kết nối H/2 duy nhất.
// ReadIdleTimeout và PingTimeout đảm bảo connection được giữ sống khi idle.
type CallbackHTTPClient struct {
	client *http.Client

	mu           sync.Mutex
	preconnURL   string    // URL đã preconnect (rỗng nếu chưa gọi Preconnect)
	preconnOK    bool      // true nếu lần preconnect gần nhất thành công
	preconnErr   string    // thông báo lỗi nếu preconnect thất bại
	preconnAt    time.Time // thời điểm preconnect được thực hiện
}

// PreconnectStatus là snapshot trạng thái preconnect, dùng cho health/connections API.
type PreconnectStatus struct {
	URL          string    `json:"url"`
	Connected    bool      `json:"connected"`
	Error        string    `json:"error,omitempty"`
	PreconnectAt time.Time `json:"preconnect_at,omitempty"`
}

// PreconnectState trả về snapshot trạng thái preconnect tại thời điểm gọi.
func (c *CallbackHTTPClient) PreconnectState() PreconnectStatus {
	c.mu.Lock()
	defer c.mu.Unlock()
	return PreconnectStatus{
		URL:          c.preconnURL,
		Connected:    c.preconnOK,
		Error:        c.preconnErr,
		PreconnectAt: c.preconnAt,
	}
}

// NewCallbackHTTPClient tạo shared client với H/2 keep-alive settings.
//   - timeout: max duration mỗi HTTP request (gồm cả retry)
//   - readIdleTimeout: gửi PING sau khoảng idle này; 0 = không gửi PING
//   - pingTimeout: đóng connection nếu không nhận PONG; 0 = dùng default golang
func NewCallbackHTTPClient(timeout, readIdleTimeout, pingTimeout time.Duration) *CallbackHTTPClient {
	h2c := &http2.Transport{
		AllowHTTP:       true,
		ReadIdleTimeout: readIdleTimeout,
		PingTimeout:     pingTimeout,
		DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, addr)
		},
	}
	h2tls := &http2.Transport{
		ReadIdleTimeout: readIdleTimeout,
		PingTimeout:     pingTimeout,
	}
	return &CallbackHTTPClient{
		client: &http.Client{
			Transport: &dualH2Transport{h2c: h2c, h2tls: h2tls},
			Timeout:   timeout,
		},
	}
}

// Preconnect thiết lập kết nối H/2 tới host của rawURL ngay lập tức.
// Gọi khi app khởi động để warm up connection pool trước khi có session đầu tiên.
// Không fatal nếu server chưa sẵn sàng — chỉ log warning.
func (c *CallbackHTTPClient) Preconnect(ctx context.Context, rawURL string) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		slog.Warn("callback: preconnect: invalid url", "url", rawURL, "err", err)
		c.recordPreconnect(rawURL, false, err.Error())
		return
	}
	target := parsed.Scheme + "://" + parsed.Host + "/"
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, target, nil)
	if err != nil {
		slog.Warn("callback: preconnect: build request failed", "url", target, "err", err)
		c.recordPreconnect(target, false, err.Error())
		return
	}
	resp, err := c.client.Do(req)
	if err != nil {
		slog.Warn("callback: preconnect failed — will retry on first callback", "url", target, "err", err)
		c.recordPreconnect(target, false, err.Error())
		return
	}
	resp.Body.Close()
	slog.Info("callback: preconnected via HTTP/2", "url", target, "status", resp.StatusCode)
	c.recordPreconnect(target, true, "")
}

func (c *CallbackHTTPClient) recordPreconnect(url string, ok bool, errMsg string) {
	c.mu.Lock()
	c.preconnURL = url
	c.preconnOK = ok
	c.preconnErr = errMsg
	c.preconnAt = time.Now()
	c.mu.Unlock()
}

// NewHTTPCallbackSink tạo sink dùng shared HTTP/2 client.
// Dùng trong production — nhiều session cùng callback host tái sử dụng 1 kết nối H/2.
func (c *CallbackHTTPClient) NewHTTPCallbackSink(rawURL string, maxRetry int) *HTTPCallbackSink {
	return &HTTPCallbackSink{
		URL:          rawURL,
		MaxRetry:     maxRetry,
		RetryBackoff: 200 * time.Millisecond,
		client:       c.client,
	}
}

// callbackPayload bọc RecognitionResult với field event_type cho callback body.
// Embedding đảm bảo tất cả field của RecognitionResult được flatten vào JSON.
// ContextID và TerminationID được flatten từ MediaResources.TAccess để consumer
// không phải dig vào nested object.
type callbackPayload struct {
	EventType     string `json:"event_type"`
	ContextID     string `json:"contextId,omitempty"`
	TerminationID string `json:"terminationId,omitempty"`
	pipeline.RecognitionResult
}

func eventTypeOf(r pipeline.RecognitionResult) string {
	if r.IsFinal {
		return "asr.transcript.final"
	}
	return "asr.transcript.partial"
}

func newCallbackPayload(r pipeline.RecognitionResult) callbackPayload {
	p := callbackPayload{
		EventType:         eventTypeOf(r),
		RecognitionResult: r,
	}
	if r.MediaResources != nil {
		mr := r.MediaResources.TAccess
		if mr == nil || mr.ContextID == "" {
			mr = r.MediaResources.TCore
		}
		if mr != nil {
			p.ContextID = mr.ContextID
			p.TerminationID = mr.TerminationID
		}
	}
	return p
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
	body, err := json.Marshal(newCallbackPayload(r))
	if err != nil {
		return fmt.Errorf("http_callback: marshal: %w", err)
	}

	slog.Debug("callback: sending",
		"url", s.URL,
		"session_id", r.SessionID,
		"is_final", r.IsFinal,
		"seq", r.Seq,
		"body", string(body),
	)

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
