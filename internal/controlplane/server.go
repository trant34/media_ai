// Package controlplane là HTTP/2 control plane của Media AI Gateway.
//
// Hai chế độ hoạt động:
//   - h2c (HTTP/2 cleartext): CertFile bỏ trống, phù hợp cho backend sau proxy TLS.
//   - TLS + HTTP/2:           CertFile + KeyFile được cấu hình, ALPN tự negotiate h2.
//
// Routes:
//
//	POST   /v1/sessions              — tạo session + khởi động pipeline
//	GET    /v1/sessions/{id}         — lấy thông tin session
//	DELETE /v1/sessions/{id}         — đóng session
//	POST   /v1/webrtc/offer          — WebRTC SDP offer/answer (source_type=webrtc)
//	PUT    /v1/gateways/{id}/heartbeat — gateway node gửi trạng thái định kỳ
//	GET    /health/live              — liveness probe (luôn 200 nếu process chạy)
//	GET    /health/ready             — readiness probe (503 nếu quá tải)
//	GET    /health                   — tương đương /health/live (backward compat)
//	GET    /metrics                  — Prometheus-compatible metrics scrape
//	GET    /v1/stats                 — aggregate stats (JSON)
package controlplane

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"media-ai-gateway/internal/ai"
	"media-ai-gateway/internal/ingress/rawrtp"
	wrtc "media-ai-gateway/internal/ingress/webrtc"
	"media-ai-gateway/internal/pipeline"
	"media-ai-gateway/internal/result"
	"media-ai-gateway/internal/session"
)

const defaultRegistryTTL = 30 * time.Second

// Starter là interface được implement bởi *coordinator.Coordinator.
type Starter interface {
	Start(sess *session.Session) (*result.HTTPCallbackSink, error)
}

// ServerConfig cấu hình HTTP/2 control plane.
type ServerConfig struct {
	Addr         string // ví dụ: ":8080"
	CertFile     string // PEM cert; nếu rỗng → dùng h2c
	KeyFile      string // PEM key; nếu rỗng → dùng h2c
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	IdleTimeout  time.Duration

	// Identity
	GatewayID string // returned in CREATE response so clients know which node

	// Per-session RTP port allocation (§5.4).
	// If RTPPortStart == 0, port allocation is disabled (shared ingress only).
	RTPPublicIP  string // IP advertised to the caller (may differ from bind IP)
	RTPBindIP    string // local IP to bind per-session UDP sockets; "" = all interfaces
	RTPPortStart int    // inclusive lower bound of the port pool
	RTPPortEnd   int    // inclusive upper bound of the port pool

	// RegistryTTL là thời gian tối đa một gateway node được coi là fresh sau lần heartbeat cuối.
	// 0 → sử dụng mặc định 30s.
	RegistryTTL time.Duration

	// WebRTC configures the WebRTC ingress (Pion PeerConnection, ICE servers, NAT).
	// Used to handle POST /v1/webrtc/offer.
	WebRTC wrtc.Config
}

// DefaultServerConfig trả về cấu hình mặc định (h2c, port 8080).
func DefaultServerConfig() ServerConfig {
	return ServerConfig{
		Addr:         ":8080",
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
}

// Server là HTTP/2 control plane cho Media AI Gateway.
type Server struct {
	cfg        ServerConfig
	sessionMgr *session.Manager
	coord      Starter
	pool       *pipeline.WorkerPool
	aiMgr      *ai.Manager
	dispatcher *result.Dispatcher
	portAlloc  *PortAllocator
	admission  *AdmissionController
	registry   *GatewayRegistry
	selector   *GatewaySelector
	webrtc     *wrtc.Handler
	inner      *http.Server

	// Optional metric sources — set after construction via setter methods.
	ingress       *rawrtp.Ingress
	cbMu          sync.Mutex
	httpCallbacks []*result.HTTPCallbackSink
}

// SetRTPIngress wires shared Raw RTP ingress cho metric collection tại /metrics.
func (s *Server) SetRTPIngress(ingress *rawrtp.Ingress) { s.ingress = ingress }

// RegisterCallbackSink đăng ký HTTPCallbackSink để expose callback_retry_total tại /metrics.
// Goroutine-safe: được gọi từ HTTP handler goroutines đồng thời.
func (s *Server) RegisterCallbackSink(sink *result.HTTPCallbackSink) {
	s.cbMu.Lock()
	s.httpCallbacks = append(s.httpCallbacks, sink)
	s.cbMu.Unlock()
}

// SetWorkerRegistry wires AI WorkerRegistry vào AdmissionController để kiểm tra reachability.
// Readiness sẽ fail (reason: "no_ai_worker") khi không có fresh worker nào trong registry.
func (s *Server) SetWorkerRegistry(r *ai.WorkerRegistry) { s.admission.SetWorkerRegistry(r) }

// SetMemThreshold cấu hình ngưỡng heap allocation (bytes) cho readiness check.
// Readiness sẽ fail (reason: "memory_pressure") khi heap vượt ngưỡng. 0 = disabled.
func (s *Server) SetMemThreshold(bytes uint64) { s.admission.SetMemThreshold(bytes) }

// MetricsHandler trả về http.Handler phục vụ chỉ endpoint /metrics.
// Dùng khi MetricsAddr khác HTTPAddr để chạy server metrics riêng.
func (s *Server) MetricsHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		s.metricsWrite(w)
	})
}

// NewServer tạo Server và đăng ký tất cả routes.
// Nếu cfg.RTPPortStart > 0, một PortAllocator được tạo tự động; panic nếu range không hợp lệ.
func NewServer(
	cfg ServerConfig,
	sessionMgr *session.Manager,
	coord Starter,
	pool *pipeline.WorkerPool,
	aiMgr *ai.Manager,
	dispatcher *result.Dispatcher,
) *Server {
	var portAlloc *PortAllocator
	if cfg.RTPPortStart > 0 {
		var err error
		portAlloc, err = NewPortAllocator(cfg.RTPPortStart, cfg.RTPPortEnd)
		if err != nil {
			panic("controlplane: invalid RTP port range: " + err.Error())
		}
	}

	ttl := cfg.RegistryTTL
	if ttl == 0 {
		ttl = defaultRegistryTTL
	}
	registry := NewGatewayRegistry(ttl)
	selector := NewGatewaySelector(registry)

	webrtcAPI, err := wrtc.NewAPI(cfg.WebRTC)
	if err != nil {
		panic("controlplane: WebRTC API init failed: " + err.Error())
	}

	webrtcHandler := wrtc.NewHandler(webrtcAPI, cfg.WebRTC, sessionMgr)
	webrtcHandler.SetDispatcher(dispatcher)

	s := &Server{
		cfg:        cfg,
		sessionMgr: sessionMgr,
		coord:      coord,
		pool:       pool,
		aiMgr:      aiMgr,
		dispatcher: dispatcher,
		portAlloc:  portAlloc,
		admission:  NewAdmissionController(sessionMgr, pool, aiMgr, portAlloc),
		registry:   registry,
		selector:   selector,
		webrtc:     webrtcHandler,
	}

	// Auto-register self so this node appears in the registry immediately.
	if cfg.GatewayID != "" {
		registry.Register(s.selfInfo())
	}

	var handler http.Handler = s.routes()

	if cfg.CertFile == "" {
		handler = h2c.NewHandler(handler, &http2.Server{})
	}

	s.inner = &http.Server{
		Addr:         cfg.Addr,
		Handler:      handler,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		IdleTimeout:  cfg.IdleTimeout,
	}

	if cfg.CertFile != "" {
		_ = http2.ConfigureServer(s.inner, nil)
	}

	return s
}

// routes đăng ký tất cả REST endpoints lên gin.Engine.
func (s *Server) routes() http.Handler {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery(), ginLogger())

	v1 := r.Group("/v1")
	{
		v1.POST("/sessions", s.createSession)
		v1.GET("/sessions/:id", s.getSession)
		v1.DELETE("/sessions/:id", s.deleteSession)
		v1.POST("/webrtc/offer", gin.WrapF(s.webrtc.ServeOffer))
		v1.PUT("/gateways/:id/heartbeat", s.heartbeat)
		v1.GET("/stats", s.stats)
	}

	r.GET("/health/live", s.healthLive)
	r.GET("/health/ready", s.healthReady)
	r.GET("/health", s.healthLive) // backward compat
	r.GET("/metrics", s.metrics)

	return r
}

// selfInfo snapshots this node's current resource state for the GatewayRegistry.
func (s *Server) selfInfo() GatewayInfo {
	var usedPorts int
	if s.portAlloc != nil {
		usedPorts = s.portAlloc.Total() - s.portAlloc.Available()
	}
	var audioQueueUsage float64
	if cap := s.pool.QueueCap(); cap > 0 {
		audioQueueUsage = float64(s.pool.QueueLen()) / float64(cap)
	}
	return GatewayInfo{
		ID:              s.cfg.GatewayID,
		Addr:            s.cfg.Addr,
		RTPPublicIP:     s.cfg.RTPPublicIP,
		Sessions:        s.sessionMgr.Count(),
		MaxSessions:     s.sessionMgr.MaxSessions(),
		AIStreams:       s.aiMgr.Count(),
		MaxAIStreams:    s.aiMgr.MaxStreams(),
		AudioQueueUsage: audioQueueUsage,
		RTPPortStart:    s.cfg.RTPPortStart,
		RTPPortEnd:      s.cfg.RTPPortEnd,
		UsedPorts:       usedPorts,
		LastHeartbeatMs: time.Now().UnixMilli(),
	}
}

// ListenAndServe khởi động server, block cho đến khi ctx bị cancel hoặc lỗi fatal.
func (s *Server) ListenAndServe(ctx context.Context) error {
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = s.inner.Shutdown(shutCtx)
	}()

	if s.cfg.CertFile != "" {
		return s.inner.ListenAndServeTLS(s.cfg.CertFile, s.cfg.KeyFile)
	}
	return s.inner.ListenAndServe()
}
