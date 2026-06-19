package controlplane

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"time"

	"github.com/gin-gonic/gin"

	"media-ai-gateway/internal/ingress/rawrtp"
	"media-ai-gateway/internal/session"
)

func (s *Server) createSession(c *gin.Context) {
	var req CreateSessionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "invalid JSON: " + err.Error()})
		return
	}

	switch {
	case req.ID == "":
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "id is required"})
		return
	case req.Codec == "":
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "codec is required"})
		return
	case req.SampleRate == 0:
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "sample_rate is required"})
		return
	}

	switch req.SourceType {
	case "", "raw_rtp", "webrtc":
		// valid
	default:
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "source_type must be 'raw_rtp' or 'webrtc'"})
		return
	}

	if ok, retryMs := s.admission.Check(); !ok {
		c.JSON(http.StatusServiceUnavailable, ErrorResponse{Error: "service at capacity, try again later", RetryAfterMs: retryMs})
		return
	}

	channels := req.Channels
	if channels == 0 {
		channels = 1
	}

	sess, err := s.sessionMgr.Create(session.SessionConfig{
		ID:          req.ID,
		SourceType:  req.SourceType,
		SSRC:        req.SSRC,
		PayloadType: req.PayloadType,
		Codec:       req.Codec,
		SampleRate:  req.SampleRate,
		Channels:    channels,
		Language:    req.Language,
		Task:        req.Task,
		CallbackURL: req.CallbackURL,
		RemoteAddr:  req.RemoteAddr,
	})
	if err != nil {
		switch {
		case errors.Is(err, session.ErrDuplicateID):
			c.JSON(http.StatusConflict, ErrorResponse{Error: err.Error()})
		case errors.Is(err, session.ErrMaxSessions):
			c.JSON(http.StatusServiceUnavailable, ErrorResponse{Error: err.Error(), RetryAfterMs: 5000})
		default:
			c.JSON(http.StatusInternalServerError, ErrorResponse{Error: err.Error()})
		}
		return
	}
	sess.GatewayID = s.cfg.GatewayID

	// Allocate a dedicated UDP port for raw_rtp sessions (§5.4).
	var rtpPort int
	if req.SourceType == "raw_rtp" && s.portAlloc != nil {
		port, err := s.portAlloc.Acquire()
		if err != nil {
			s.sessionMgr.Close(sess.ID)
			c.JSON(http.StatusServiceUnavailable, ErrorResponse{Error: "no RTP ports available", RetryAfterMs: 10000})
			return
		}
		if err := rawrtp.StartSessionListener(sess, s.cfg.RTPBindIP, port, s.portAlloc); err != nil {
			// Bind failed before goroutine started — release manually.
			s.portAlloc.Release(port)
			s.sessionMgr.Close(sess.ID)
			c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "rtp: " + err.Error()})
			return
		}
		rtpPort = port
	}

	cbSink, err := s.coord.Start(sess)
	if err != nil {
		// Closing the session cancels sess.Ctx, which causes the listener goroutine
		// to exit and release the port asynchronously.
		s.sessionMgr.Close(sess.ID)
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "pipeline: " + err.Error()})
		return
	}
	if cbSink != nil {
		s.RegisterCallbackSink(cbSink)
	}

	resp := sessionToResponse(sess)
	resp.GatewayID = s.cfg.GatewayID
	if rtpPort != 0 {
		resp.RTPIP = s.cfg.RTPPublicIP
		resp.RTPPort = rtpPort
	}
	c.JSON(http.StatusCreated, resp)
}

func (s *Server) getSession(c *gin.Context) {
	sess, ok := s.sessionMgr.Get(c.Param("id"))
	if !ok {
		c.JSON(http.StatusNotFound, ErrorResponse{Error: "session not found"})
		return
	}
	c.JSON(http.StatusOK, sessionToResponse(sess))
}

func (s *Server) deleteSession(c *gin.Context) {
	if !s.sessionMgr.Close(c.Param("id")) {
		c.JSON(http.StatusNotFound, ErrorResponse{Error: "session not found"})
		return
	}
	c.Status(http.StatusNoContent)
}

// heartbeat handles PUT /v1/gateways/:id/heartbeat.
func (s *Server) heartbeat(c *gin.Context) {
	var info GatewayInfo
	if err := c.ShouldBindJSON(&info); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "invalid JSON: " + err.Error()})
		return
	}
	id := c.Param("id")
	if info.ID != "" && info.ID != id {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "id in body does not match URL path"})
		return
	}
	info.ID = id
	s.registry.Register(info)
	if s.cfg.GatewayID != "" {
		s.registry.Register(s.selfInfo())
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// healthLive handles GET /health/live — Kubernetes liveness probe.
func (s *Server) healthLive(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// healthReady handles GET /health/ready — Kubernetes readiness probe.
func (s *Server) healthReady(c *gin.Context) {
	ok, retryMs, reason := s.admission.CheckWithReason()
	if !ok {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status":         "not_ready",
			"reason":         reason,
			"retry_after_ms": retryMs,
		})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ready"})
}

// metrics handles GET /metrics — Prometheus text exposition format (version 0.0.4).
func (s *Server) metrics(c *gin.Context) {
	w := c.Writer
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	s.metricsWrite(w)
}

// metricsWrite writes Prometheus text output to w.
func (s *Server) metricsWrite(w io.Writer) {
	ps := s.pool.Stats()
	ds := s.dispatcher.Stats()
	now := time.Now().UnixMilli()

	fmt.Fprintf(w, "# HELP media_ai_sessions_active Active session count\n")
	fmt.Fprintf(w, "# TYPE media_ai_sessions_active gauge\n")
	fmt.Fprintf(w, "media_ai_sessions_active %d\n", s.sessionMgr.Count())

	fmt.Fprintf(w, "# HELP media_ai_sessions_max Maximum allowed session count\n")
	fmt.Fprintf(w, "# TYPE media_ai_sessions_max gauge\n")
	fmt.Fprintf(w, "media_ai_sessions_max %d\n", s.sessionMgr.MaxSessions())

	fmt.Fprintf(w, "# HELP media_ai_ai_streams_active Active AI gRPC stream count\n")
	fmt.Fprintf(w, "# TYPE media_ai_ai_streams_active gauge\n")
	fmt.Fprintf(w, "media_ai_ai_streams_active %d\n", s.aiMgr.Count())

	fmt.Fprintf(w, "# HELP media_ai_ai_streams_max Maximum allowed AI stream count\n")
	fmt.Fprintf(w, "# TYPE media_ai_ai_streams_max gauge\n")
	fmt.Fprintf(w, "media_ai_ai_streams_max %d\n", s.aiMgr.MaxStreams())

	fmt.Fprintf(w, "# HELP media_ai_pool_sessions_active Per-session pipelines registered in worker pool\n")
	fmt.Fprintf(w, "# TYPE media_ai_pool_sessions_active gauge\n")
	fmt.Fprintf(w, "media_ai_pool_sessions_active %d\n", s.pool.SessionCount())

	fmt.Fprintf(w, "# HELP media_ai_pool_queue_len Worker pool pending job count\n")
	fmt.Fprintf(w, "# TYPE media_ai_pool_queue_len gauge\n")
	fmt.Fprintf(w, "media_ai_pool_queue_len %d\n", s.pool.QueueLen())

	fmt.Fprintf(w, "# HELP media_ai_pool_queue_cap Worker pool job queue capacity\n")
	fmt.Fprintf(w, "# TYPE media_ai_pool_queue_cap gauge\n")
	fmt.Fprintf(w, "media_ai_pool_queue_cap %d\n", s.pool.QueueCap())

	fmt.Fprintf(w, "# HELP media_ai_pool_submitted_total Total jobs submitted to worker pool\n")
	fmt.Fprintf(w, "# TYPE media_ai_pool_submitted_total counter\n")
	fmt.Fprintf(w, "media_ai_pool_submitted_total %d\n", ps.Submitted)

	fmt.Fprintf(w, "# HELP media_ai_pool_dropped_total Total jobs dropped from worker pool\n")
	fmt.Fprintf(w, "# TYPE media_ai_pool_dropped_total counter\n")
	fmt.Fprintf(w, "media_ai_pool_dropped_total %d\n", ps.Dropped)

	fmt.Fprintf(w, "# HELP media_ai_pool_processed_total Total jobs processed by worker pool\n")
	fmt.Fprintf(w, "# TYPE media_ai_pool_processed_total counter\n")
	fmt.Fprintf(w, "media_ai_pool_processed_total %d\n", ps.Processed)

	fmt.Fprintf(w, "# HELP media_ai_pool_decode_errors_total Total decode errors in worker pool\n")
	fmt.Fprintf(w, "# TYPE media_ai_pool_decode_errors_total counter\n")
	fmt.Fprintf(w, "media_ai_pool_decode_errors_total %d\n", ps.DecodeErrors)

	fmt.Fprintf(w, "# HELP media_ai_dispatcher_queue_len Result dispatcher pending queue depth\n")
	fmt.Fprintf(w, "# TYPE media_ai_dispatcher_queue_len gauge\n")
	fmt.Fprintf(w, "media_ai_dispatcher_queue_len %d\n", s.dispatcher.QueueLen())

	fmt.Fprintf(w, "# HELP media_ai_dispatcher_sent_total Total results sent via callback\n")
	fmt.Fprintf(w, "# TYPE media_ai_dispatcher_sent_total counter\n")
	fmt.Fprintf(w, "media_ai_dispatcher_sent_total %d\n", ds.Sent)

	fmt.Fprintf(w, "# HELP media_ai_dispatcher_send_errors_total Total callback send errors\n")
	fmt.Fprintf(w, "# TYPE media_ai_dispatcher_send_errors_total counter\n")
	fmt.Fprintf(w, "media_ai_dispatcher_send_errors_total %d\n", ds.SendErrors)

	if s.portAlloc != nil {
		fmt.Fprintf(w, "# HELP media_ai_rtp_ports_available Free RTP ports in pool\n")
		fmt.Fprintf(w, "# TYPE media_ai_rtp_ports_available gauge\n")
		fmt.Fprintf(w, "media_ai_rtp_ports_available %d\n", s.portAlloc.Available())

		fmt.Fprintf(w, "# HELP media_ai_rtp_ports_total Total RTP ports in pool\n")
		fmt.Fprintf(w, "# TYPE media_ai_rtp_ports_total gauge\n")
		fmt.Fprintf(w, "media_ai_rtp_ports_total %d\n", s.portAlloc.Total())
	}

	// --- AI stream errors ---
	aiSt := s.aiMgr.Stats()
	fmt.Fprintf(w, "# HELP media_ai_ai_send_errors_total Total AI gRPC send errors\n")
	fmt.Fprintf(w, "# TYPE media_ai_ai_send_errors_total counter\n")
	fmt.Fprintf(w, "media_ai_ai_send_errors_total %d\n", aiSt.TotalSendErrors)

	fmt.Fprintf(w, "# HELP media_ai_ai_recv_errors_total Total AI gRPC recv errors\n")
	fmt.Fprintf(w, "# TYPE media_ai_ai_recv_errors_total counter\n")
	fmt.Fprintf(w, "media_ai_ai_recv_errors_total %d\n", aiSt.TotalRecvErrors)

	fmt.Fprintf(w, "# HELP media_ai_ai_reconnects_total Total AI gRPC stream reconnect attempts\n")
	fmt.Fprintf(w, "# TYPE media_ai_ai_reconnects_total counter\n")
	fmt.Fprintf(w, "media_ai_ai_reconnects_total %d\n", aiSt.TotalRetries)

	// --- Result metrics ---
	fmt.Fprintf(w, "# HELP media_ai_result_partial_total Total partial recognition results delivered\n")
	fmt.Fprintf(w, "# TYPE media_ai_result_partial_total counter\n")
	fmt.Fprintf(w, "media_ai_result_partial_total %d\n", ds.SentPartial)

	fmt.Fprintf(w, "# HELP media_ai_result_final_total Total final recognition results delivered\n")
	fmt.Fprintf(w, "# TYPE media_ai_result_final_total counter\n")
	fmt.Fprintf(w, "media_ai_result_final_total %d\n", ds.SentFinal)

	fmt.Fprintf(w, "# HELP media_ai_result_queue_dropped_total Total results dropped from dispatcher queue\n")
	fmt.Fprintf(w, "# TYPE media_ai_result_queue_dropped_total counter\n")
	fmt.Fprintf(w, "media_ai_result_queue_dropped_total %d\n", ds.Dropped)

	// --- Callback retries ---
	var totalCallbackRetries uint64
	s.cbMu.Lock()
	for _, cb := range s.httpCallbacks {
		totalCallbackRetries += cb.Retries()
	}
	s.cbMu.Unlock()
	fmt.Fprintf(w, "# HELP media_ai_callback_retry_total Total HTTP callback retry attempts\n")
	fmt.Fprintf(w, "# TYPE media_ai_callback_retry_total counter\n")
	fmt.Fprintf(w, "media_ai_callback_retry_total %d\n", totalCallbackRetries)

	// --- RTP ingress ---
	if s.ingress != nil {
		ist := s.ingress.Stats()
		fmt.Fprintf(w, "# HELP media_ai_rtp_packets_total Total RTP packets received from UDP\n")
		fmt.Fprintf(w, "# TYPE media_ai_rtp_packets_total counter\n")
		fmt.Fprintf(w, "media_ai_rtp_packets_total %d\n", ist.Received)

		fmt.Fprintf(w, "# HELP media_ai_rtp_packets_routed_total RTP packets successfully routed to session\n")
		fmt.Fprintf(w, "# TYPE media_ai_rtp_packets_routed_total counter\n")
		fmt.Fprintf(w, "media_ai_rtp_packets_routed_total %d\n", ist.Routed)

		fmt.Fprintf(w, "# HELP media_ai_rtp_queue_dropped_total RTP packets dropped: session queue full\n")
		fmt.Fprintf(w, "# TYPE media_ai_rtp_queue_dropped_total counter\n")
		fmt.Fprintf(w, "media_ai_rtp_queue_dropped_total %d\n", ist.DroppedQueueFull)

		fmt.Fprintf(w, "# HELP media_ai_rtp_unknown_ssrc_total RTP packets dropped: no matching session SSRC\n")
		fmt.Fprintf(w, "# TYPE media_ai_rtp_unknown_ssrc_total counter\n")
		fmt.Fprintf(w, "media_ai_rtp_unknown_ssrc_total %d\n", ist.DroppedUnknownSSRC)

		fmt.Fprintf(w, "# HELP media_ai_rtp_parse_errors_total RTP packets dropped: parse error\n")
		fmt.Fprintf(w, "# TYPE media_ai_rtp_parse_errors_total counter\n")
		fmt.Fprintf(w, "media_ai_rtp_parse_errors_total %d\n", ist.DroppedParseError)
	}

	// --- System metrics ---
	fmt.Fprintf(w, "# HELP media_ai_goroutines_current Current number of goroutines\n")
	fmt.Fprintf(w, "# TYPE media_ai_goroutines_current gauge\n")
	fmt.Fprintf(w, "media_ai_goroutines_current %d\n", runtime.NumGoroutine())

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	fmt.Fprintf(w, "# HELP media_ai_memory_usage_bytes Current heap allocation in bytes\n")
	fmt.Fprintf(w, "# TYPE media_ai_memory_usage_bytes gauge\n")
	fmt.Fprintf(w, "media_ai_memory_usage_bytes %d\n", ms.Alloc)

	fmt.Fprintf(w, "# HELP media_ai_gateway_nodes_registered Registered gateway nodes in registry\n")
	fmt.Fprintf(w, "# TYPE media_ai_gateway_nodes_registered gauge\n")
	fmt.Fprintf(w, "media_ai_gateway_nodes_registered %d\n", s.registry.Len())

	fmt.Fprintf(w, "# HELP media_ai_scrape_timestamp_ms Unix millisecond timestamp of this scrape\n")
	fmt.Fprintf(w, "# TYPE media_ai_scrape_timestamp_ms gauge\n")
	fmt.Fprintf(w, "media_ai_scrape_timestamp_ms %d\n", now)
}

func (s *Server) stats(c *gin.Context) {
	ps := s.pool.Stats()
	ds := s.dispatcher.Stats()
	aiSt := s.aiMgr.Stats()
	c.JSON(http.StatusOK, StatsResponse{
		Sessions: s.sessionMgr.Count(),
		AIStreams: s.aiMgr.Count(),
		Pool: PoolStats{
			Submitted:      ps.Submitted,
			Dropped:        ps.Dropped,
			Processed:      ps.Processed,
			DecodeErrors:   ps.DecodeErrors,
			ActiveSessions: s.pool.SessionCount(),
			QueueLen:       s.pool.QueueLen(),
		},
		Dispatcher: DispatcherStats{
			Pushed:      ds.Pushed,
			Dropped:     ds.Dropped,
			Sent:        ds.Sent,
			SentPartial: ds.SentPartial,
			SentFinal:   ds.SentFinal,
			SendErrors:  ds.SendErrors,
			QueueLen:    s.dispatcher.QueueLen(),
		},
		AI: AIStats{
			TotalSendErrors: aiSt.TotalSendErrors,
			TotalRecvErrors: aiSt.TotalRecvErrors,
			TotalRetries:    aiSt.TotalRetries,
		},
	})
}

func sessionToResponse(sess *session.Session) SessionResponse {
	return SessionResponse{
		SessionID:  sess.ID,
		Status:     string(sess.Status()),
		SourceType: sess.SourceType,
		Codec:      sess.Codec,
		SampleRate: sess.SampleRate,
		Channels:   sess.Channels,
		Language:   sess.Language,
		Task:       sess.Task,
		CreatedAt:  sess.CreatedAt,
	}
}
