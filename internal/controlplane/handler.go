package controlplane

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"media-ai-gateway/internal/ingress/rawrtp"
	"media-ai-gateway/internal/pipeline"
	"media-ai-gateway/internal/session"
)

// notifyEvent handles POST /v1/vonras/call-sessions/:callId/notify-event.
// Dispatches on sessionEvent.event: BEGIN → 200 OK; ANSWER → create session + allocate RTP port.
func (s *Server) notifyEvent(c *gin.Context) {
	if zap.L().Core().Enabled(zap.DebugLevel) {
		raw, _ := io.ReadAll(c.Request.Body)
		c.Request.Body = io.NopCloser(bytes.NewReader(raw))
		zap.L().Debug("dcsf→dcas: notify-event raw", zap.String("call_id", c.Param("callId")), zap.ByteString("body", raw))
	}

	var event SessionEvent
	if err := c.ShouldBindJSON(&event); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "invalid JSON: " + err.Error()})
		return
	}
	callID := c.Param("callId")
	zap.L().Debug("dcsf→dcas: notify-event", zap.String("call_id", callID), zap.String("event", event.Event), zap.String("service", event.SelectedService), zap.String("callback_url", event.CallbackURL))
	switch strings.ToUpper(event.Event) {
	case "BEGIN":
		c.JSON(http.StatusOK, gin.H{})
	case "ANSWER":
		s.handleAnswer(c, callID, &event)
	default:
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "unknown event: " + event.Event})
	}
}

// handleAnswer processes ANSWER event: create 2 sessions (tcore + taccess) + allocate 2 RTP ports.
// Each call maps to 2 internal sessions: {callID}-tcore and {callID}-taccess.
func (s *Server) handleAnswer(c *gin.Context, callID string, event *SessionEvent) {
	codec, sampleRate, supported := serviceToCodec(event.SelectedService)
	if !supported {
		// Service not yet handled (e.g. fun_calling) — acknowledge without creating session.
		c.JSON(http.StatusOK, gin.H{})
		return
	}

	if ok, retryMs := s.admission.Check(); !ok {
		c.JSON(http.StatusServiceUnavailable, ErrorResponse{Error: "service at capacity, try again later", RetryAfterMs: retryMs})
		return
	}

	tcoreID := callID + "-tcore"
	taccessID := callID + "-taccess"
	baseCfg := session.SessionConfig{
		SourceType:  "raw_rtp",
		Codec:       codec,
		SampleRate:  sampleRate,
		Channels:    1,
		Task:        event.SelectedService,
		CallbackURL: s.cfg.ResultCallbackURL,
	}

	baseCfg.ID = tcoreID
	sessTCore, err := s.sessionMgr.Create(baseCfg)
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
	sessTCore.GatewayID = s.cfg.GatewayID

	baseCfg.ID = taccessID
	sessAccess, err := s.sessionMgr.Create(baseCfg)
	if err != nil {
		s.sessionMgr.Close(tcoreID)
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
	sessAccess.GatewayID = s.cfg.GatewayID

	var tcorePort, taccessPort int
	if s.portAlloc != nil {
		p1, err := s.portAlloc.Acquire()
		if err != nil {
			s.sessionMgr.Close(tcoreID)
			s.sessionMgr.Close(taccessID)
			c.JSON(http.StatusServiceUnavailable, ErrorResponse{Error: "no RTP ports available", RetryAfterMs: 10000})
			return
		}
		p2, err := s.portAlloc.Acquire()
		if err != nil {
			s.portAlloc.Release(p1)
			s.sessionMgr.Close(tcoreID)
			s.sessionMgr.Close(taccessID)
			c.JSON(http.StatusServiceUnavailable, ErrorResponse{Error: "no RTP ports available", RetryAfterMs: 10000})
			return
		}
		if err := rawrtp.StartSessionListener(sessTCore, s.cfg.RTPBindIP, p1, s.portAlloc); err != nil {
			s.portAlloc.Release(p1)
			s.portAlloc.Release(p2)
			s.sessionMgr.Close(tcoreID)
			s.sessionMgr.Close(taccessID)
			c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "rtp: " + err.Error()})
			return
		}
		if err := rawrtp.StartSessionListener(sessAccess, s.cfg.RTPBindIP, p2, s.portAlloc); err != nil {
			// tcore listener goroutine running — closing tcoreID cancels its ctx → releases p1
			s.portAlloc.Release(p2)
			s.sessionMgr.Close(tcoreID)
			s.sessionMgr.Close(taccessID)
			c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "rtp: " + err.Error()})
			return
		}
		tcorePort = p1
		taccessPort = p2
	}

	cbSink1, err := s.coord.Start(sessTCore)
	if err != nil {
		s.sessionMgr.Close(tcoreID)
		s.sessionMgr.Close(taccessID)
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "pipeline: " + err.Error()})
		return
	}
	cbSink2, err := s.coord.Start(sessAccess)
	if err != nil {
		s.sessionMgr.Close(tcoreID)
		s.sessionMgr.Close(taccessID)
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "pipeline: " + err.Error()})
		return
	}
	if cbSink1 != nil {
		s.RegisterCallbackSink(cbSink1)
	}
	if cbSink2 != nil {
		s.RegisterCallbackSink(cbSink2)
	}

	resp := sessionToResponse(sessTCore)
	resp.SessionID = callID
	resp.GatewayID = s.cfg.GatewayID
	if tcorePort != 0 {
		resp.RTPIP = s.cfg.RTPPublicIP
		resp.TCoreRTPPort = tcorePort
		resp.TAccessRTPPort = taccessPort
		resp.TCoreLocalNonDcMedia = buildNonDcMedia(codec, sampleRate, 1, tcorePort, 0)
		resp.TAccessLocalNonDcMedia = buildNonDcMedia(codec, sampleRate, 1, taccessPort, 0)
	}
	zap.L().Debug("dcas→dcsf: answer ack", zap.String("call_id", callID), zap.String("rtp_ip", resp.RTPIP), zap.Int("tcore_rtp_port", resp.TCoreRTPPort), zap.Int("taccess_rtp_port", resp.TAccessRTPPort))
	c.JSON(http.StatusOK, resp)
	c.Writer.Flush()

	if event.CallbackURL != "" {
		timeout := s.cfg.DCSFCallControlTimeout
		if timeout == 0 {
			timeout = 30 * time.Second
		}
		ctrlResultURL := s.cfg.PublicURL + "/v1/vonras/call-sessions/" + callID + "/ctrl-result"
		zap.L().Debug("dcas→dcsf: call-control", zap.String("call_id", callID), zap.String("target", event.CallbackURL), zap.String("service", event.SelectedService), zap.String("ctrl_result_url", ctrlResultURL))
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			if err := s.dcsfPool.SendCallControl(ctx, event.CallbackURL, callID, event.SelectedService, ctrlResultURL); err != nil {
				zap.L().Warn("call-control failed, releasing session", zap.String("call_id", callID), zap.Error(err))
				s.sessionMgr.Close(tcoreID)
				s.sessionMgr.Close(taccessID)
			}
		}()
	}
}

// serviceToCodec maps DCSF selectedService to codec + sampleRate.
// Returns ok=false for services not yet handled (caller should ACK without creating session).
func serviceToCodec(service string) (codec string, sampleRate int, ok bool) {
	switch strings.ToLower(service) {
	case "realtime_translation", "speech_to_text":
		return "PCMU", 8000, true
	default:
		return "", 0, false
	}
}

// getCallSession handles GET /v1/vonras/call-sessions/:callId.
// Looks up {callId}-taccess first (subscriber side), falls back to {callId}-tcore.
func (s *Server) getCallSession(c *gin.Context) {
	callID := c.Param("callId")
	sess, ok := s.sessionMgr.Get(callID + "-taccess")
	if !ok {
		sess, ok = s.sessionMgr.Get(callID + "-tcore")
	}
	if !ok {
		c.JSON(http.StatusNotFound, ErrorResponse{Error: "session not found"})
		return
	}
	resp := sessionToResponse(sess)
	resp.SessionID = callID
	c.JSON(http.StatusOK, resp)
}

// ctrlResult handles POST /v1/vonras/call-sessions/:callId/ctrl-result.
// Updates mediaResources and per-termination callbackUrl for both tcore and taccess sessions.
func (s *Server) ctrlResult(c *gin.Context) {
	if zap.L().Core().Enabled(zap.DebugLevel) {
		raw, _ := io.ReadAll(c.Request.Body)
		c.Request.Body = io.NopCloser(bytes.NewReader(raw))
		zap.L().Debug("dcsf→dcas: ctrl-result raw", zap.String("call_id", c.Param("callId")), zap.ByteString("body", raw))
	}

	var req CtrlResultRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "invalid JSON: " + err.Error()})
		return
	}
	id := c.Param("callId")
	zap.L().Debug("dcsf→dcas: ctrl-result", zap.String("call_id", id), zap.Bool("has_media_resources", req.MediaResources != nil))

	if req.MediaResources != nil {
		zap.L().Debug("dcsf→dcas: ctrl-result media",
			zap.String("call_id",         id),
			zap.String("tcore_ctx",       req.MediaResources.TCore.ContextID),
			zap.String("tcore_term",      req.MediaResources.TCore.Termination.TerminationID),
			zap.String("tcore_callback",  req.MediaResources.TCore.CallbackURL),
			zap.String("taccess_ctx",     req.MediaResources.TAccess.ContextID),
			zap.String("taccess_term",    req.MediaResources.TAccess.Termination.TerminationID),
			zap.String("taccess_callback", req.MediaResources.TAccess.CallbackURL),
		)

		tcorePatch := session.SessionPatch{
			CallbackURL: req.MediaResources.TCore.CallbackURL,
			MediaResources: &pipeline.MediaResources{
				TCore: &pipeline.MediaResource{
					ContextID:     req.MediaResources.TCore.ContextID,
					TerminationID: req.MediaResources.TCore.Termination.TerminationID,
				},
			},
		}
		if _, err := s.sessionMgr.Update(id+"-tcore", tcorePatch); err == nil {
			if cb := s.coord.UpdateCallbackSink(id+"-tcore", req.MediaResources.TCore.CallbackURL); cb != nil {
				s.RegisterCallbackSink(cb)
			}
			if s.cfg.MockResultPump {
				if sessTCore, ok := s.sessionMgr.Get(id + "-tcore"); ok {
					s.coord.StartMockResultPump(sessTCore, req.MediaResources.TCore.Termination.TerminationID)
				}
			}
		}

		taccessPatch := session.SessionPatch{
			CallbackURL: req.MediaResources.TAccess.CallbackURL,
			MediaResources: &pipeline.MediaResources{
				TAccess: &pipeline.MediaResource{
					ContextID:     req.MediaResources.TAccess.ContextID,
					TerminationID: req.MediaResources.TAccess.Termination.TerminationID,
				},
			},
		}
		if _, err := s.sessionMgr.Update(id+"-taccess", taccessPatch); err == nil {
			if cb := s.coord.UpdateCallbackSink(id+"-taccess", req.MediaResources.TAccess.CallbackURL); cb != nil {
				s.RegisterCallbackSink(cb)
			}
			if s.cfg.MockResultPump {
				if sessAccess, ok := s.sessionMgr.Get(id + "-taccess"); ok {
					s.coord.StartMockResultPump(sessAccess, req.MediaResources.TAccess.Termination.TerminationID)
				}
			}
		}
	}

	sess, ok := s.sessionMgr.Get(id + "-taccess")
	if !ok {
		sess, ok = s.sessionMgr.Get(id + "-tcore")
	}
	if !ok {
		c.JSON(http.StatusNotFound, ErrorResponse{Error: "session not found"})
		return
	}
	resp := sessionToResponse(sess)
	resp.SessionID = id
	c.JSON(http.StatusOK, resp)
}

// deleteCallSession handles DELETE /v1/vonras/call-sessions/:callId.
// Closes both {callId}-tcore and {callId}-taccess sessions.
func (s *Server) deleteCallSession(c *gin.Context) {
	callID := c.Param("callId")
	zap.L().Debug("dcsf→dcas: delete call-session", zap.String("call_id", callID))
	closedCore := s.sessionMgr.Close(callID + "-tcore")
	closedAccess := s.sessionMgr.Close(callID + "-taccess")
	if !closedCore && !closedAccess {
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

	fmt.Fprintf(w, "# HELP media_ai_sessions_created_total Total sessions created since startup\n")
	fmt.Fprintf(w, "# TYPE media_ai_sessions_created_total counter\n")
	fmt.Fprintf(w, "media_ai_sessions_created_total %d\n", s.sessionMgr.CreatedTotal())

	fmt.Fprintf(w, "# HELP media_ai_sessions_closed_total Total sessions closed since startup\n")
	fmt.Fprintf(w, "# TYPE media_ai_sessions_closed_total counter\n")
	fmt.Fprintf(w, "media_ai_sessions_closed_total %d\n", s.sessionMgr.ClosedTotal())

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

	fmt.Fprintf(w, "# HELP media_ai_dispatcher_pushed_total Total results pushed into dispatcher queue\n")
	fmt.Fprintf(w, "# TYPE media_ai_dispatcher_pushed_total counter\n")
	fmt.Fprintf(w, "media_ai_dispatcher_pushed_total %d\n", ds.Pushed)

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

	fmt.Fprintf(w, "# HELP media_ai_ai_first_result_ms_total Cumulative sum of first-result latencies per stream (ms)\n")
	fmt.Fprintf(w, "# TYPE media_ai_ai_first_result_ms_total counter\n")
	fmt.Fprintf(w, "media_ai_ai_first_result_ms_total %d\n", aiSt.LatencyFirstSum)

	fmt.Fprintf(w, "# HELP media_ai_ai_first_result_count_total Number of streams that produced at least one result\n")
	fmt.Fprintf(w, "# TYPE media_ai_ai_first_result_count_total counter\n")
	fmt.Fprintf(w, "media_ai_ai_first_result_count_total %d\n", aiSt.LatencyFirstCount)

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

	// --- Jitter buffer aggregate (type assert — graceful nếu coord không implement) ---
	type jitterStatsGetter interface {
		AggregateJitterStats() (received, released, dropped, lost uint64)
	}
	if js, ok := s.coord.(jitterStatsGetter); ok {
		recv, rel, drop, lost := js.AggregateJitterStats()
		fmt.Fprintf(w, "# HELP media_ai_jitter_received_total Total RTP packets received by jitter buffers\n")
		fmt.Fprintf(w, "# TYPE media_ai_jitter_received_total counter\n")
		fmt.Fprintf(w, "media_ai_jitter_received_total %d\n", recv)

		fmt.Fprintf(w, "# HELP media_ai_jitter_released_total Total packets released from jitter buffers in order\n")
		fmt.Fprintf(w, "# TYPE media_ai_jitter_released_total counter\n")
		fmt.Fprintf(w, "media_ai_jitter_released_total %d\n", rel)

		fmt.Fprintf(w, "# HELP media_ai_jitter_dropped_total Total packets dropped (late arrival, duplicate, output full)\n")
		fmt.Fprintf(w, "# TYPE media_ai_jitter_dropped_total counter\n")
		fmt.Fprintf(w, "media_ai_jitter_dropped_total %d\n", drop)

		fmt.Fprintf(w, "# HELP media_ai_jitter_lost_total Total detected sequence gaps (packets never arrived)\n")
		fmt.Fprintf(w, "# TYPE media_ai_jitter_lost_total counter\n")
		fmt.Fprintf(w, "media_ai_jitter_lost_total %d\n", lost)
	}

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

func (s *Server) connections(c *gin.Context) {
	resp := ConnectionsResponse{}

	// AI gRPC workers.
	if s.grpcPool != nil {
		aiSt := s.aiMgr.Stats()
		for _, addr := range s.grpcPool.Addrs() {
			resp.AIWorkers = append(resp.AIWorkers, AIWorkerConn{
				Addr:         addr,
				State:        s.grpcPool.State(addr).String(),
				ActiveStream: s.aiMgr.Count(),
				Latency: AILatencyStats{
					AvgFirstResultMs: aiSt.AvgFirstResultMs(),
				},
			})
		}
	}

	// Callback HTTP/2.
	if s.callbackClient != nil {
		st := s.callbackClient.PreconnectState()
		resp.Callback = CallbackConn{
			URL:       st.URL,
			Connected: st.Connected,
			Error:     st.Error,
		}
		if !st.PreconnectAt.IsZero() {
			resp.Callback.PreconnectAt = st.PreconnectAt.UTC().Format(time.RFC3339)
		}
	}

	// RTP connections.
	if s.portAlloc != nil {
		total := s.portAlloc.Total()
		avail := s.portAlloc.Available()
		resp.RTP = RTPConnSummary{
			PerSessionOpen:     total - avail,
			PerSessionCapacity: total,
			SharedIngress:      s.ingress != nil,
		}
	} else {
		resp.RTP = RTPConnSummary{
			SharedIngress: s.ingress != nil,
		}
	}

	c.JSON(http.StatusOK, resp)
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

// buildNonDcMedia xây dựng SDP mô tả RTP endpoint của gateway (dùng cho 3GPP MRM API).
// DCAS dùng kết quả này làm remoteNonDcMedia khi gọi MF MRM POST /contexts.
func buildNonDcMedia(codec string, sampleRate, channels, rtpPort int, payloadType uint8) *NonDcMedia {
	upper := strings.ToUpper(codec)

	ch := channels
	if ch <= 0 {
		ch = 1
	}

	pt := int(payloadType) // 0 = dùng default của codec

	var maxptime int
	var rtpmap, fmtp string

	switch upper {
	case "PCMU":
		// PT 0 là static assignment cho PCMU — không thay đổi dù payloadType=0
		maxptime = 20
		if ch > 1 {
			rtpmap = fmt.Sprintf("rtpmap:%d PCMU/%d/%d", pt, sampleRate, ch)
		} else {
			rtpmap = fmt.Sprintf("rtpmap:%d PCMU/%d", pt, sampleRate)
		}

	case "PCMA":
		if pt == 0 {
			pt = 8
		}
		maxptime = 20
		if ch > 1 {
			rtpmap = fmt.Sprintf("rtpmap:%d PCMA/%d/%d", pt, sampleRate, ch)
		} else {
			rtpmap = fmt.Sprintf("rtpmap:%d PCMA/%d", pt, sampleRate)
		}

	case "OPUS":
		if pt == 0 {
			pt = 111
		}
		if ch < 2 {
			ch = 2 // Opus default 2 channels trong SDP
		}
		maxptime = 60
		rtpmap = fmt.Sprintf("rtpmap:%d opus/%d/%d", pt, sampleRate, ch)
		fmtp = fmt.Sprintf("fmtp:%d minptime=10;useinbandfec=1", pt)

	case "AMR", "AMR-NB":
		if pt == 0 {
			pt = 96
		}
		maxptime = 80
		rtpmap = fmt.Sprintf("rtpmap:%d AMR/8000", pt)
		fmtp = fmt.Sprintf("fmtp:%d octet-align=1", pt)

	case "AMR-WB", "AMRWB":
		if pt == 0 {
			pt = 97
		}
		maxptime = 80
		rtpmap = fmt.Sprintf("rtpmap:%d AMR-WB/16000", pt)
		fmtp = fmt.Sprintf("fmtp:%d octet-align=1", pt)

	case "EVS":
		if pt == 0 {
			pt = 100
		}
		if sampleRate == 0 {
			sampleRate = 16000
		}
		maxptime = 100
		rtpmap = fmt.Sprintf("rtpmap:%d EVS/%d", pt, sampleRate)
		bw := "wb"
		if sampleRate <= 8000 {
			bw = "nb"
		}
		fmtp = fmt.Sprintf("fmtp:%d br=13.2;bw=%s", pt, bw)

	default:
		if pt == 0 {
			pt = 96
		}
		rtpmap = fmt.Sprintf("rtpmap:%d %s/%d", pt, codec, sampleRate)
	}

	if maxptime == 0 {
		maxptime = 20
	}

	aLines := []string{rtpmap}
	if fmtp != "" {
		aLines = append(aLines, fmtp)
	}
	aLines = append(aLines, "ptime:20", fmt.Sprintf("maxptime:%d", maxptime), "recvonly")

	return &NonDcMedia{
		SDPMLine:  fmt.Sprintf("audio %d RTP/AVP %d", rtpPort, pt),
		SDPALines: aLines,
	}
}
