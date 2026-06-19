// Command media-ai-gateway là entry point của Media AI Gateway.
//
// Khởi động:
//
//	media-ai-gateway [--config path.yaml]
//	media-ai-gateway [flags]
//
// Khi --config được cung cấp, tất cả cấu hình đọc từ YAML file.
// Khi không có --config, dùng các flag riêng lẻ (backward compatible).
//
// Flags (mỗi flag có env var tương ứng GATEWAY_<FLAG_UPPER>):
//
//	--config           string   Path to YAML config file
//	--http-addr        string   HTTP/2 control plane (default ":8080")
//	--http-cert        string   TLS cert PEM — nếu rỗng dùng h2c cleartext
//	--http-key         string   TLS key PEM
//	--rtp-addr         string   Raw RTP UDP shared listen (default ":5004")
//	--gateway-id       string   Node identity returned in CREATE response
//	--rtp-public-ip    string   Public IP advertised to callers for per-session RTP
//	--rtp-bind-ip      string   Local IP to bind per-session UDP sockets (default "")
//	--rtp-port-start   int      First port in per-session port pool (0 = disabled)
//	--rtp-port-end     int      Last port in per-session port pool
//	--chunk-ms         int      Chunk duration gửi AI (ms, default 500)
//	--out-rate         int      Resample target Hz (default 16000)
//	--workers          int      Audio pipeline worker count (default 16)
//	--max-sess         int      Max concurrent sessions (default 10000)
//	--log-level        string   debug|info|warn|error (default info)
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"media-ai-gateway/internal/ai"
	"media-ai-gateway/internal/config"
	"media-ai-gateway/internal/controlplane"
	"media-ai-gateway/internal/coordinator"
	"media-ai-gateway/internal/ingress/rawrtp"
	"media-ai-gateway/internal/pipeline"
	"media-ai-gateway/internal/result"
	"media-ai-gateway/internal/session"
)

func main() {
	// ── flags ──────────────────────────────────────────────────────────────
	configFile   := flag.String("config",         envOr("GATEWAY_CONFIG",        ""),      "Path to YAML config file (overrides individual flags)")
	httpAddr     := flag.String("http-addr",       envOr("GATEWAY_HTTP_ADDR",     ":8080"), "HTTP/2 control plane address")
	httpCert     := flag.String("http-cert",       envOr("GATEWAY_HTTP_CERT",     ""),      "TLS cert PEM (empty = h2c)")
	httpKey      := flag.String("http-key",        envOr("GATEWAY_HTTP_KEY",      ""),      "TLS key PEM")
	rtpAddr      := flag.String("rtp-addr",        envOr("GATEWAY_RTP_ADDR",      ":5004"), "Raw RTP UDP shared listen address")
	gatewayID    := flag.String("gateway-id",      envOr("GATEWAY_ID",            ""),      "Node identity returned in CREATE response")
	rtpPublicIP  := flag.String("rtp-public-ip",   envOr("GATEWAY_RTP_PUBLIC_IP", ""),      "Public IP advertised to callers for per-session RTP")
	rtpBindIP    := flag.String("rtp-bind-ip",     envOr("GATEWAY_RTP_BIND_IP",   ""),      "Local IP to bind per-session UDP sockets")
	rtpPortStart := flag.Int("rtp-port-start",     0,     "First port in per-session pool (0 = disabled)")
	rtpPortEnd   := flag.Int("rtp-port-end",       0,     "Last port in per-session pool")
	chunkMs      := flag.Int("chunk-ms",           500,   "Audio chunk duration sent to AI (ms)")
	outRate      := flag.Int("out-rate",           16000, "Resample output sample rate (Hz)")
	workers      := flag.Int("workers",            16,    "Audio pipeline worker pool size")
	maxSess      := flag.Int("max-sess",           10000, "Maximum concurrent sessions")
	logLevel     := flag.String("log-level",       "info","Log level: debug|info|warn|error")
	flag.Parse()

	// ── config ─────────────────────────────────────────────────────────────
	var cfg config.GatewayConfig
	if *configFile != "" {
		var err error
		cfg, err = config.Load(*configFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to load config %q: %v\n", *configFile, err)
			os.Exit(1)
		}
	} else {
		cfg = config.Default()
		cfg.Server.HTTPAddr          = *httpAddr
		cfg.Server.CertFile          = *httpCert
		cfg.Server.KeyFile           = *httpKey
		cfg.RTP.ListenAddr           = *rtpAddr
		cfg.Gateway.ID               = *gatewayID
		cfg.RTP.PublicIP             = *rtpPublicIP
		cfg.RTP.BindIP               = *rtpBindIP
		cfg.RTP.PortStart            = *rtpPortStart
		cfg.RTP.PortEnd              = *rtpPortEnd
		cfg.Audio.ChunkMs            = *chunkMs
		cfg.Audio.OutputSampleRate   = *outRate
		cfg.Pipeline.AudioWorkerCount = *workers
		cfg.Session.MaxSessions      = *maxSess
		cfg.Log.Level                = *logLevel
	}

	// ── logger ─────────────────────────────────────────────────────────────
	logger := buildLogger(cfg.Log.Level)
	slog.SetDefault(logger)

	slog.Info("media-ai-gateway starting",
		"http_addr",      cfg.Server.HTTPAddr,
		"rtp_addr",       cfg.RTP.ListenAddr,
		"gateway_id",     cfg.Gateway.ID,
		"rtp_public_ip",  cfg.RTP.PublicIP,
		"rtp_port_range", [2]int{cfg.RTP.PortStart, cfg.RTP.PortEnd},
		"chunk_ms",       cfg.Audio.ChunkMs,
		"out_rate",       cfg.Audio.OutputSampleRate,
		"workers",        cfg.Pipeline.AudioWorkerCount,
		"max_sess",       cfg.Session.MaxSessions,
		"tls",            cfg.Server.CertFile != "",
		"h2c",            cfg.Server.CertFile == "",
	)

	// ── components ─────────────────────────────────────────────────────────
	sessMgr := session.NewManager(cfg.ToSessionManagerConfig())
	pool    := pipeline.NewWorkerPool(cfg.ToWorkerPoolConfig())

	// WorkerRegistry: lưu AI worker nodes.
	// TTL=0 → static worker từ config không bao giờ expire.
	workerReg := ai.NewWorkerRegistry(0)

	var aiDialer ai.Dialer
	if cfg.AI.GRPCTarget != "" {
		// Seed registry với static worker từ config.
		// Khi AI worker tự đăng ký qua heartbeat, entry này bị override bởi thông tin live.
		workerReg.Register(ai.WorkerInfo{
			ID:        "config:" + cfg.AI.GRPCTarget,
			Addr:      cfg.AI.GRPCTarget,
			MaxStreams: cfg.AI.MaxActiveStreams,
		})
		aiDialer = ai.NewRoutingDialer(workerReg, ai.NewGRPCDialFunc())
		slog.Info("AI routing dialer active", "target", cfg.AI.GRPCTarget)
	} else {
		// Dev/smoke-test mode: pipeline chạy đầy đủ nhưng không có transcript.
		aiDialer = ai.NullDialer{}
		slog.Warn("AI grpc_target not configured — using NullDialer, no transcripts will be generated")
	}
	aiMgr := ai.NewManager(cfg.ToAIConfig(), aiDialer)

	disp := result.NewDispatcher(cfg.ToDispatcherConfig())

	coord := coordinator.New(cfg.ToCoordinatorConfig(), pool, aiMgr, disp)

	rtpRouter  := rawrtp.NewManagerRouter(sessMgr)
	rtpIngress := rawrtp.NewIngress(cfg.ToRTPIngressConfig(), rtpRouter)

	apiSrv := controlplane.NewServer(cfg.ToControlPlaneServerConfig(), sessMgr, coord, pool, aiMgr, disp)
	apiSrv.SetRTPIngress(rtpIngress)
	// Wire WorkerRegistry chỉ khi dùng RoutingDialer thực.
	// NullDialer (dev mode): bỏ qua để admission không block vì "no_ai_worker".
	if cfg.AI.GRPCTarget != "" {
		apiSrv.SetWorkerRegistry(workerReg)
	}

	// ── run ────────────────────────────────────────────────────────────────
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go pool.Run(ctx)
	go disp.Run(ctx)
	go sessMgr.Run(ctx)

	go func() {
		slog.Info("RTP ingress listening", "addr", cfg.RTP.ListenAddr)
		if err := rtpIngress.Run(ctx); err != nil {
			slog.Error("RTP ingress error", "err", err)
			stop()
		}
	}()

	// Separate metrics server when MetricsAddr is configured.
	if cfg.Server.MetricsAddr != "" && cfg.Server.MetricsAddr != cfg.Server.HTTPAddr {
		go serveMetrics(ctx, cfg.Server.MetricsAddr, apiSrv.MetricsHandler())
	}

	httpDone := make(chan error, 1)
	go func() {
		mode := "h2c"
		if cfg.Server.CertFile != "" {
			mode = "tls+h2"
		}
		slog.Info("HTTP/2 control plane listening", "addr", cfg.Server.HTTPAddr, "mode", mode)
		httpDone <- apiSrv.ListenAndServe(ctx)
	}()

	// ── shutdown ───────────────────────────────────────────────────────────
	<-ctx.Done()
	slog.Info("signal received, shutting down")

	select {
	case err := <-httpDone:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("HTTP server exit error", "err", err)
		}
	case <-time.After(15 * time.Second):
		slog.Warn("HTTP server shutdown timed out")
	}

	slog.Info("shutdown complete")
}

// serveMetrics runs a minimal HTTP server at addr serving only /metrics.
func serveMetrics(ctx context.Context, addr string, h http.Handler) {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", h)
	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		slog.Error("metrics server listen failed", "addr", addr, "err", err)
		return
	}
	slog.Info("metrics server listening", "addr", addr)
	if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("metrics server error", "err", err)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func buildLogger(level string) *slog.Logger {
	var l slog.Level
	if err := l.UnmarshalText([]byte(level)); err != nil {
		fmt.Fprintf(os.Stderr, "unknown log level %q, using info\n", level)
		l = slog.LevelInfo
	}
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: l})
	return slog.New(h)
}
