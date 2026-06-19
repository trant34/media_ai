package controlplane

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/http2"

	"media-ai-gateway/internal/ai"
	"media-ai-gateway/internal/pipeline"
	"media-ai-gateway/internal/result"
	"media-ai-gateway/internal/session"
)

// ---------- fakes ----------

type fakeStarter struct {
	err     error
	started []string
}

func (f *fakeStarter) Start(sess *session.Session) (*result.HTTPCallbackSink, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.started = append(f.started, sess.ID)
	return nil, nil
}

type noopDialer struct{}

func (noopDialer) Dial(_ context.Context, _, _, _, _ string) (ai.StreamClient, error) {
	return nil, fmt.Errorf("noop")
}

// ---------- test stack ----------

func newTestServer(t *testing.T, coord Starter) *Server {
	t.Helper()
	mgr := session.NewManager(session.ManagerConfig{
		MaxSessions:     10,
		PacketQueueSize: 8,
		AudioQueueSize:  8,
		ResultQueueSize: 8,
		IdleTimeout:     time.Minute,
		GCInterval:      time.Minute,
	})
	pool := pipeline.NewWorkerPool(pipeline.WorkerPoolConfig{Workers: 0, QueueSize: 8})
	aiMgr := ai.NewManager(ai.Config{MaxActiveStreams: 10, StreamTimeout: time.Minute}, noopDialer{})
	disp := result.NewDispatcher(result.DefaultConfig())

	return NewServer(DefaultServerConfig(), mgr, coord, pool, aiMgr, disp)
}

func postJSON(t *testing.T, s *Server, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	b, _ := json.Marshal(body)
	r := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(b))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	s.routes().ServeHTTP(w, r)
	return w
}

func getPath(t *testing.T, s *Server, path string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, path, nil)
	w := httptest.NewRecorder()
	s.routes().ServeHTTP(w, r)
	return w
}

func deletePath(t *testing.T, s *Server, path string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodDelete, path, nil)
	w := httptest.NewRecorder()
	s.routes().ServeHTTP(w, r)
	return w
}

func decodeJSON(t *testing.T, w *httptest.ResponseRecorder, v any) {
	t.Helper()
	if err := json.NewDecoder(w.Body).Decode(v); err != nil {
		t.Fatalf("decode JSON: %v", err)
	}
}

func assertStatus(t *testing.T, w *httptest.ResponseRecorder, want int) {
	t.Helper()
	if w.Code != want {
		t.Errorf("status: want %d, got %d — body: %s", want, w.Code, w.Body.String())
	}
}

func assertContentType(t *testing.T, w *httptest.ResponseRecorder) {
	t.Helper()
	ct := w.Header().Get("Content-Type")
	if !strings.HasPrefix(ct, "application/json") {
		t.Errorf("Content-Type: want application/json, got %q", ct)
	}
}

func validCreateReq() CreateSessionRequest {
	return CreateSessionRequest{
		ID:         "sess-1",
		SourceType: "raw_rtp",
		Codec:      "PCMU",
		SampleRate: 8000,
		Channels:   1,
	}
}

// ---------- POST /v1/sessions ----------

func TestCreateSession_Created(t *testing.T) {
	coord := &fakeStarter{}
	s := newTestServer(t, coord)

	w := postJSON(t, s, "/v1/sessions", validCreateReq())

	assertStatus(t, w, http.StatusCreated)
	assertContentType(t, w)

	var resp SessionResponse
	decodeJSON(t, w, &resp)
	if resp.SessionID != "sess-1" {
		t.Errorf("session_id: want sess-1, got %s", resp.SessionID)
	}
	if resp.Codec != "PCMU" {
		t.Errorf("codec: want PCMU, got %s", resp.Codec)
	}
	if resp.SampleRate != 8000 {
		t.Errorf("sample_rate: want 8000, got %d", resp.SampleRate)
	}
	if resp.Status == "" {
		t.Error("status should not be empty")
	}
	if coord.started[0] != "sess-1" {
		t.Errorf("coordinator not called with sess-1")
	}
}

func TestCreateSession_DefaultChannels(t *testing.T) {
	coord := &fakeStarter{}
	s := newTestServer(t, coord)

	req := validCreateReq()
	req.Channels = 0
	w := postJSON(t, s, "/v1/sessions", req)
	assertStatus(t, w, http.StatusCreated)

	var resp SessionResponse
	decodeJSON(t, w, &resp)
	if resp.Channels != 1 {
		t.Errorf("channels: want 1 (default), got %d", resp.Channels)
	}
}

func TestCreateSession_MissingID(t *testing.T) {
	s := newTestServer(t, &fakeStarter{})
	req := validCreateReq()
	req.ID = ""
	w := postJSON(t, s, "/v1/sessions", req)
	assertStatus(t, w, http.StatusBadRequest)
	var e ErrorResponse
	decodeJSON(t, w, &e)
	if !strings.Contains(e.Error, "id") {
		t.Errorf("error should mention id, got: %s", e.Error)
	}
}

func TestCreateSession_MissingCodec(t *testing.T) {
	s := newTestServer(t, &fakeStarter{})
	req := validCreateReq()
	req.Codec = ""
	w := postJSON(t, s, "/v1/sessions", req)
	assertStatus(t, w, http.StatusBadRequest)
}

func TestCreateSession_MissingSampleRate(t *testing.T) {
	s := newTestServer(t, &fakeStarter{})
	req := validCreateReq()
	req.SampleRate = 0
	w := postJSON(t, s, "/v1/sessions", req)
	assertStatus(t, w, http.StatusBadRequest)
}

func TestCreateSession_InvalidJSON(t *testing.T) {
	s := newTestServer(t, &fakeStarter{})
	r := httptest.NewRequest(http.MethodPost, "/v1/sessions", strings.NewReader("{bad json"))
	w := httptest.NewRecorder()
	s.routes().ServeHTTP(w, r)
	assertStatus(t, w, http.StatusBadRequest)
}

func TestCreateSession_DuplicateID(t *testing.T) {
	s := newTestServer(t, &fakeStarter{})
	postJSON(t, s, "/v1/sessions", validCreateReq())
	w := postJSON(t, s, "/v1/sessions", validCreateReq())
	assertStatus(t, w, http.StatusConflict)
}

func TestCreateSession_CoordFail_RollsBack(t *testing.T) {
	coord := &fakeStarter{err: fmt.Errorf("pipeline broke")}
	s := newTestServer(t, coord)
	w := postJSON(t, s, "/v1/sessions", validCreateReq())
	assertStatus(t, w, http.StatusBadRequest)

	wg := getPath(t, s, "/v1/sessions/sess-1")
	assertStatus(t, wg, http.StatusNotFound)
}

func TestCreateSession_MaxSessions(t *testing.T) {
	mgr := session.NewManager(session.ManagerConfig{
		MaxSessions:     1,
		PacketQueueSize: 8,
		AudioQueueSize:  8,
		ResultQueueSize: 8,
		IdleTimeout:     time.Minute,
		GCInterval:      time.Minute,
	})
	pool := pipeline.NewWorkerPool(pipeline.WorkerPoolConfig{Workers: 0, QueueSize: 8})
	aiMgr := ai.NewManager(ai.Config{MaxActiveStreams: 10, StreamTimeout: time.Minute}, noopDialer{})
	disp := result.NewDispatcher(result.DefaultConfig())
	s := NewServer(DefaultServerConfig(), mgr, &fakeStarter{}, pool, aiMgr, disp)

	postJSON(t, s, "/v1/sessions", validCreateReq())

	req2 := validCreateReq()
	req2.ID = "sess-2"
	w := postJSON(t, s, "/v1/sessions", req2)
	assertStatus(t, w, http.StatusServiceUnavailable)
}

// ---------- GET /v1/sessions/{id} ----------

func TestGetSession_Found(t *testing.T) {
	s := newTestServer(t, &fakeStarter{})
	postJSON(t, s, "/v1/sessions", validCreateReq())

	w := getPath(t, s, "/v1/sessions/sess-1")
	assertStatus(t, w, http.StatusOK)
	assertContentType(t, w)

	var resp SessionResponse
	decodeJSON(t, w, &resp)
	if resp.SessionID != "sess-1" {
		t.Errorf("session_id: want sess-1, got %s", resp.SessionID)
	}
	if resp.Codec != "PCMU" {
		t.Errorf("codec mismatch")
	}
}

func TestGetSession_NotFound(t *testing.T) {
	s := newTestServer(t, &fakeStarter{})
	w := getPath(t, s, "/v1/sessions/nope")
	assertStatus(t, w, http.StatusNotFound)
}

// ---------- DELETE /v1/sessions/{id} ----------

func TestDeleteSession_NoContent(t *testing.T) {
	s := newTestServer(t, &fakeStarter{})
	postJSON(t, s, "/v1/sessions", validCreateReq())

	w := deletePath(t, s, "/v1/sessions/sess-1")
	assertStatus(t, w, http.StatusNoContent)
}

func TestDeleteSession_NotFound(t *testing.T) {
	s := newTestServer(t, &fakeStarter{})
	w := deletePath(t, s, "/v1/sessions/nope")
	assertStatus(t, w, http.StatusNotFound)
}

func TestDeleteSession_GoneAfterDelete(t *testing.T) {
	s := newTestServer(t, &fakeStarter{})
	postJSON(t, s, "/v1/sessions", validCreateReq())
	deletePath(t, s, "/v1/sessions/sess-1")

	w := getPath(t, s, "/v1/sessions/sess-1")
	assertStatus(t, w, http.StatusNotFound)
}

// ---------- GET /health ----------

func TestHealthReady_OK(t *testing.T) {
	s := newTestServer(t, &fakeStarter{})
	w := getPath(t, s, "/health/ready")
	assertStatus(t, w, http.StatusOK)

	var body map[string]string
	decodeJSON(t, w, &body)
	if body["status"] != "ready" {
		t.Errorf("status: want ready, got %s", body["status"])
	}
}

func TestHealthReady_NotReady_HasReason(t *testing.T) {
	// Force rejection via memory threshold = 1 byte.
	s := newTestServer(t, &fakeStarter{})
	s.SetMemThreshold(1)

	w := getPath(t, s, "/health/ready")
	assertStatus(t, w, http.StatusServiceUnavailable)

	var body map[string]any
	decodeJSON(t, w, &body)
	if body["status"] != "not_ready" {
		t.Errorf("status: want not_ready, got %v", body["status"])
	}
	if body["reason"] == "" || body["reason"] == nil {
		t.Error("reason should be non-empty when not ready")
	}
	if body["retry_after_ms"] == nil {
		t.Error("retry_after_ms should be present when not ready")
	}
}

func TestHealthReady_WorkerRegistry_NoWorker_NotReady(t *testing.T) {
	s := newTestServer(t, &fakeStarter{})
	reg := ai.NewWorkerRegistry(0) // empty
	s.SetWorkerRegistry(reg)

	w := getPath(t, s, "/health/ready")
	assertStatus(t, w, http.StatusServiceUnavailable)

	var body map[string]any
	decodeJSON(t, w, &body)
	if body["reason"] != "no_ai_worker" {
		t.Errorf("reason: want no_ai_worker, got %v", body["reason"])
	}
}

func TestHealth_OK(t *testing.T) {
	s := newTestServer(t, &fakeStarter{})
	w := getPath(t, s, "/health")
	assertStatus(t, w, http.StatusOK)
	assertContentType(t, w)

	var body map[string]string
	decodeJSON(t, w, &body)
	if body["status"] != "ok" {
		t.Errorf("status: want ok, got %s", body["status"])
	}
}

// ---------- GET /v1/stats ----------

func TestStats_OK(t *testing.T) {
	s := newTestServer(t, &fakeStarter{})
	postJSON(t, s, "/v1/sessions", validCreateReq())

	w := getPath(t, s, "/v1/stats")
	assertStatus(t, w, http.StatusOK)
	assertContentType(t, w)

	var resp StatsResponse
	decodeJSON(t, w, &resp)
	if resp.Sessions != 1 {
		t.Errorf("sessions: want 1, got %d", resp.Sessions)
	}
}

// ---------- GET /metrics ----------

func TestMetrics_ContainsRequiredMetrics(t *testing.T) {
	s := newTestServer(t, &fakeStarter{})
	w := getPath(t, s, "/metrics")
	assertStatus(t, w, http.StatusOK)

	body := w.Body.String()

	required := []string{
		// existing
		"media_ai_sessions_active",
		"media_ai_sessions_max",
		"media_ai_ai_streams_active",
		"media_ai_pool_sessions_active",
		"media_ai_pool_queue_len",
		"media_ai_pool_queue_cap",
		"media_ai_pool_submitted_total",
		"media_ai_pool_dropped_total",
		"media_ai_pool_processed_total",
		"media_ai_pool_decode_errors_total",
		"media_ai_dispatcher_queue_len",
		"media_ai_dispatcher_sent_total",
		"media_ai_dispatcher_send_errors_total",
		"media_ai_gateway_nodes_registered",
		"media_ai_scrape_timestamp_ms",
		// new §5.25
		"media_ai_ai_send_errors_total",
		"media_ai_ai_recv_errors_total",
		"media_ai_ai_reconnects_total",
		"media_ai_result_partial_total",
		"media_ai_result_final_total",
		"media_ai_result_queue_dropped_total",
		"media_ai_callback_retry_total",
		"media_ai_goroutines_current",
		"media_ai_memory_usage_bytes",
	}
	for _, name := range required {
		if !strings.Contains(body, name) {
			t.Errorf("metric %q not found in /metrics output", name)
		}
	}
}

func TestMetrics_ContentType(t *testing.T) {
	s := newTestServer(t, &fakeStarter{})
	w := getPath(t, s, "/metrics")
	ct := w.Header().Get("Content-Type")
	if !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("Content-Type: want text/plain, got %q", ct)
	}
}

func TestMetrics_PoolQueueCapReflectsConfig(t *testing.T) {
	s := newTestServer(t, &fakeStarter{}) // QueueSize=8 in newTestServer
	w := getPath(t, s, "/metrics")
	body := w.Body.String()
	if !strings.Contains(body, "media_ai_pool_queue_cap 8") {
		t.Errorf("expected media_ai_pool_queue_cap 8 in metrics output, got:\n%s", body)
	}
}

// ---------- h2c integration ----------

func TestH2C_ServerServesHTTP2(t *testing.T) {
	s := newTestServer(t, &fakeStarter{})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	s.inner.Addr = addr
	go func() { _ = s.inner.Serve(ln) }()
	t.Cleanup(func() { _ = s.inner.Close() })

	h2cTransport := &http2.Transport{
		AllowHTTP: true,
		DialTLSContext: func(ctx context.Context, network, remoteAddr string, _ *tls.Config) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, remoteAddr)
		},
	}
	client := &http.Client{Transport: h2cTransport}

	resp, err := client.Get("http://" + addr + "/health")
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status: want 200, got %d", resp.StatusCode)
	}
	if resp.Proto != "HTTP/2.0" {
		t.Errorf("proto: want HTTP/2.0, got %s", resp.Proto)
	}
	if !strings.Contains(string(body), "ok") {
		t.Errorf("body: want 'ok', got %s", body)
	}
}

func TestH2C_FullSessionLifecycle(t *testing.T) {
	s := newTestServer(t, &fakeStarter{})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	s.inner.Addr = addr
	go func() { _ = s.inner.Serve(ln) }()
	t.Cleanup(func() { _ = s.inner.Close() })

	h2cTransport := &http2.Transport{
		AllowHTTP: true,
		DialTLSContext: func(ctx context.Context, network, remoteAddr string, _ *tls.Config) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, remoteAddr)
		},
	}
	client := &http.Client{Transport: h2cTransport}
	base := "http://" + addr

	body, _ := json.Marshal(validCreateReq())
	resp, err := client.Post(base+"/v1/sessions", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST status: want 201, got %d", resp.StatusCode)
	}
	if resp.Proto != "HTTP/2.0" {
		t.Errorf("POST proto: want HTTP/2.0, got %s", resp.Proto)
	}

	resp, err = client.Get(base + "/v1/sessions/sess-1")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET status: want 200, got %d", resp.StatusCode)
	}

	req, _ := http.NewRequest(http.MethodDelete, base+"/v1/sessions/sess-1", nil)
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE status: want 204, got %d", resp.StatusCode)
	}
}
