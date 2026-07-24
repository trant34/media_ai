// Package monitor cung cấp goroutine định kỳ in connection info, metrics, và AI latency.
package monitor

import (
	"context"
	"log/slog"
	"time"

	"media-ai-gateway/internal/ai"
	"media-ai-gateway/internal/pipeline"
	"media-ai-gateway/internal/result"
	"media-ai-gateway/internal/session"
)

// PortAllocator is the minimal interface for reading per-session RTP port pool state.
// Implemented by *controlplane.PortAllocator.
type PortAllocator interface {
	Total() int
	Available() int
}

// Monitor periodically logs connection info, key metrics, and AI latency via slog.
// All pointer fields are optional — pass nil to skip that subsystem.
type Monitor struct {
	Interval       time.Duration
	SessionMgr     *session.Manager
	Pool           *pipeline.WorkerPool
	AIMgr          *ai.Manager
	Dispatcher     *result.Dispatcher
	GRPCPool       *ai.SharedConnPool         // optional; nil if no gRPC target configured
	CallbackClient *result.CallbackHTTPClient // optional; nil if callback not configured
	PortAlloc      PortAllocator              // optional; nil if per-session port pool not configured
	SharedIngress  bool                       // true if shared UDP ingress is running
}

// Run starts the periodic logger. Emits one line immediately, then on each tick.
// Returns immediately if Interval <= 0. Blocks until ctx is cancelled.
func (m *Monitor) Run(ctx context.Context) {
	if m.Interval <= 0 {
		return
	}
	m.emit()
	t := time.NewTicker(m.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.emit()
		}
	}
}

func (m *Monitor) emit() {
	args := make([]any, 0, 52)

	// Sessions
	if m.SessionMgr != nil {
		args = append(args,
			"sessions_active", m.SessionMgr.Count(),
			"sessions_max", m.SessionMgr.MaxSessions(),
		)
	}

	// AI streams + latency
	if m.AIMgr != nil {
		st := m.AIMgr.Stats()
		args = append(args,
			"ai_streams_active", m.AIMgr.Count(),
			"ai_streams_max", m.AIMgr.MaxStreams(),
			"ai_send_errors", st.TotalSendErrors,
			"ai_recv_errors", st.TotalRecvErrors,
			"ai_retries", st.TotalRetries,
			"ai_first_result_avg_ms", st.AvgFirstResultMs(),
		)
	}

	// Audio worker pool
	if m.Pool != nil {
		ps := m.Pool.Stats()
		args = append(args,
			"pool_submitted", ps.Submitted,
			"pool_processed", ps.Processed,
			"pool_dropped", ps.Dropped,
			"pool_decode_errors", ps.DecodeErrors,
			"pool_queue_len", m.Pool.QueueLen(),
		)
	}

	// Result dispatcher
	if m.Dispatcher != nil {
		ds := m.Dispatcher.Stats()
		args = append(args,
			"disp_sent", ds.Sent,
			"disp_sent_final", ds.SentFinal,
			"disp_sent_partial", ds.SentPartial,
			"disp_dropped", ds.Dropped,
			"disp_send_errors", ds.SendErrors,
			"disp_queue_len", m.Dispatcher.QueueLen(),
		)
	}

	// AI gRPC connection state (one entry per worker address)
	if m.GRPCPool != nil {
		for _, addr := range m.GRPCPool.Addrs() {
			args = append(args,
				"ai_grpc_addr", addr,
				"ai_grpc_state", m.GRPCPool.State(addr).String(),
			)
		}
	}

	// Callback H/2 preconnect state
	if m.CallbackClient != nil {
		st := m.CallbackClient.PreconnectState()
		args = append(args,
			"callback_url", st.URL,
			"callback_connected", st.Connected,
		)
		if st.Error != "" {
			args = append(args, "callback_error", st.Error)
		}
	}

	// RTP port pool
	if m.PortAlloc != nil {
		total := m.PortAlloc.Total()
		avail := m.PortAlloc.Available()
		args = append(args,
			"rtp_ports_open", total-avail,
			"rtp_ports_capacity", total,
		)
	}
	args = append(args, "rtp_shared_ingress", m.SharedIngress)

	slog.Info("monitor.stats", args...)
}
