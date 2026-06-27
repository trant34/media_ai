// Package config quản lý cấu hình runtime cho Media AI Gateway.
//
// Load() đọc YAML config file; Default() trả về giá trị mặc định sẵn sàng cho production.
// Các phương thức ToXxxConfig() chuyển đổi sang config struct của từng component.
//
// Thứ tự ưu tiên: YAML file > Default.
// Field thiếu trong YAML giữ nguyên giá trị Default.
package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"

	"media-ai-gateway/internal/ai"
	"media-ai-gateway/internal/controlplane"
	"media-ai-gateway/internal/coordinator"
	rawrtp "media-ai-gateway/internal/ingress/rawrtp"
	webrtcingress "media-ai-gateway/internal/ingress/webrtc"
	"media-ai-gateway/internal/jitter"
	"media-ai-gateway/internal/pipeline"
	"media-ai-gateway/internal/result"
	"media-ai-gateway/internal/session"

)

// GatewayConfig là cấu hình đầy đủ cho Media AI Gateway.
type GatewayConfig struct {
	Gateway  GatewaySection  `yaml:"gateway"`
	Server   ServerSection   `yaml:"server"`
	RTP      RTPSection      `yaml:"rtp"`
	WebRTC   WebRTCSection   `yaml:"webrtc"`
	Session  SessionSection  `yaml:"session"`
	Pipeline PipelineSection `yaml:"pipeline"`
	Audio    AudioSection    `yaml:"audio"`
	AI       AISection       `yaml:"ai"`
	Result   ResultSection   `yaml:"result"`
	Callback CallbackSection `yaml:"callback"`
	Log      LogSection      `yaml:"log"`
}

// GatewaySection định danh node và bật/tắt ingress protocol.
type GatewaySection struct {
	Name          string `yaml:"name"`
	ID            string `yaml:"id"`
	RawRTPEnabled bool   `yaml:"raw_rtp_enabled"`
	WebRTCEnabled bool   `yaml:"webrtc_enabled"`
}

// ServerSection cấu hình HTTP/2 control plane.
type ServerSection struct {
	HTTPAddr           string `yaml:"http_addr"`
	CertFile           string `yaml:"cert_file"`
	KeyFile            string `yaml:"key_file"`
	MetricsAddr        string `yaml:"metrics_addr"`       // "" = serve /metrics tại HTTPAddr
	ShutdownTimeoutSec int    `yaml:"shutdown_timeout_sec"`
}

// RTPSection cấu hình Raw RTP UDP ingress.
type RTPSection struct {
	ListenAddr       string `yaml:"listen_addr"`
	PublicIP         string `yaml:"public_ip"`
	BindIP           string `yaml:"bind_ip"`
	PortStart        int    `yaml:"port_start"`
	PortEnd          int    `yaml:"port_end"`
	SocketReadBuffer int    `yaml:"socket_read_buffer"`
}

// WebRTCSection cấu hình WebRTC ingress và ICE.
type WebRTCSection struct {
	Enabled     bool         `yaml:"enabled"`
	STUNServers []string     `yaml:"stun_servers"`
	TURNServers []TURNServer `yaml:"turn_servers"`
	NAT1To1IPs  []string     `yaml:"nat1to1_ips"`
	ICEPortMin  uint16       `yaml:"ice_port_min"`
	ICEPortMax  uint16       `yaml:"ice_port_max"`
	DCProxyURL     string `yaml:"dc_proxy_url"`
	DCUDPProxyAddr string `yaml:"dc_udp_proxy_addr"`
}

// TURNServer mô tả một TURN server.
type TURNServer struct {
	URLs       []string `yaml:"urls"`
	Username   string   `yaml:"username"`
	Credential string   `yaml:"credential"`
}

// SessionSection giới hạn tài nguyên per-session.
type SessionSection struct {
	MaxSessions           int `yaml:"max_sessions"`
	IdleTimeoutSec        int `yaml:"idle_timeout_sec"`
	GCIntervalSec         int `yaml:"gc_interval_sec"`
	PerSessionPacketQueue int `yaml:"per_session_packet_queue"`
	PerSessionAudioQueue  int `yaml:"per_session_audio_queue"`
	PerSessionResultQueue int `yaml:"per_session_result_queue"`
}

// PipelineSection cấu hình audio worker pool và jitter buffer.
type PipelineSection struct {
	AudioWorkerCount  int `yaml:"audio_worker_count"`
	AudioJobQueueSize int `yaml:"audio_job_queue_size"`
	JitterBufferMs    int `yaml:"jitter_buffer_ms"`
	MaxPacketLateMs   int `yaml:"max_packet_late_ms"`
	PacketTimeMs      int `yaml:"packet_time_ms"`
}

// AudioSection cấu hình output audio format và chunk size.
type AudioSection struct {
	OutputSampleRate int    `yaml:"output_sample_rate"`
	OutputChannels   int    `yaml:"output_channels"`
	ChunkMs          int    `yaml:"chunk_ms"`
	PCMDumpDir       string `yaml:"pcm_dump_dir"` // "" = tắt; "data/output/pcm" = ghi PCM decode ra file để kiểm tra
}

// AISection cấu hình kết nối tới AI worker và gRPC stream.
type AISection struct {
	GRPCTarget           string `yaml:"grpc_target"`
	MaxActiveStreams      int    `yaml:"max_active_streams"`
	PerStreamQueueSize   int    `yaml:"per_stream_queue_size"`
	SendTimeoutMs        int    `yaml:"send_timeout_ms"`
	StreamTimeoutSec     int    `yaml:"stream_timeout_sec"`
	MaxRetry             int    `yaml:"max_retry"`
	RetryBackoffMs       int    `yaml:"retry_backoff_ms"`
	KeepaliveTimeSec     int    `yaml:"keepalive_time_sec"`    // gửi HTTP/2 PING sau N giây idle; 0 = tắt
	KeepaliveTimeoutSec  int    `yaml:"keepalive_timeout_sec"` // đóng conn nếu không PONG trong N giây
}

// ResultSection cấu hình Result Dispatcher.
type ResultSection struct {
	DispatcherWorkers   int  `yaml:"dispatcher_workers"`
	QueueSize           int  `yaml:"queue_size"`
	DropPartialWhenFull bool `yaml:"drop_partial_when_full"`
	SendTimeoutMs       int  `yaml:"send_timeout_ms"`
}

// CallbackSection cấu hình HTTP callback sink.
type CallbackSection struct {
	URL               string `yaml:"url"`                  // optional: pre-connect target khi app khởi động
	TimeoutMs         int    `yaml:"timeout_ms"`
	MaxRetry          int    `yaml:"max_retry"`
	RetryBackoffMs    int    `yaml:"retry_backoff_ms"`
	ReadIdleTimeoutMs int    `yaml:"read_idle_timeout_ms"` // gửi H/2 PING sau N ms idle; 0 = không ping
	PingTimeoutMs     int    `yaml:"ping_timeout_ms"`      // đóng conn nếu không nhận PONG sau N ms
}

// LogSection cấu hình logging và periodic monitor.
type LogSection struct {
	Level              string `yaml:"level"`               // debug | info | warn | error
	Format             string `yaml:"format"`              // json | text
	MonitorIntervalSec int    `yaml:"monitor_interval_sec"` // in chu kỳ stats; 0 = tắt
}

// Default trả về GatewayConfig với giá trị mặc định sẵn sàng cho production.
func Default() GatewayConfig {
	return GatewayConfig{
		Gateway: GatewaySection{
			Name:          "media-ai-gateway",
			RawRTPEnabled: true,
			WebRTCEnabled: true,
		},
		Server: ServerSection{
			HTTPAddr:           ":8080",
			MetricsAddr:        "",
			ShutdownTimeoutSec: 10,
		},
		RTP: RTPSection{
			ListenAddr:       ":5004",
			SocketReadBuffer: 4 * 1024 * 1024,
		},
		WebRTC: WebRTCSection{
			Enabled:     true,
			STUNServers: []string{"stun:stun.l.google.com:19302"},
		},
		Session: SessionSection{
			MaxSessions:           10000,
			IdleTimeoutSec:        30,
			GCIntervalSec:         5,
			PerSessionPacketQueue: 128,
			PerSessionAudioQueue:  128,
			PerSessionResultQueue: 64,
		},
		Pipeline: PipelineSection{
			AudioWorkerCount:  16,
			AudioJobQueueSize: 8192,
			JitterBufferMs:    60,
			MaxPacketLateMs:   120,
			PacketTimeMs:      20,
		},
		Audio: AudioSection{
			OutputSampleRate: 16000,
			OutputChannels:   1,
			ChunkMs:          500,
		},
		AI: AISection{
			GRPCTarget:          "ai-router:50051",
			MaxActiveStreams:     1000,
			PerStreamQueueSize:  20,
			SendTimeoutMs:       500,
			StreamTimeoutSec:    300,
			MaxRetry:            3,
			RetryBackoffMs:      1000,
			KeepaliveTimeSec:    30,
			KeepaliveTimeoutSec: 10,
		},
		Result: ResultSection{
			DispatcherWorkers:   16,
			QueueSize:           4096,
			DropPartialWhenFull: true,
			SendTimeoutMs:       2000,
		},
		Callback: CallbackSection{
			TimeoutMs:         1000,
			MaxRetry:          3,
			RetryBackoffMs:    200,
			ReadIdleTimeoutMs: 30000,
			PingTimeoutMs:     15000,
		},
		Log: LogSection{
			Level:              "info",
			Format:             "json",
			MonitorIntervalSec: 60,
		},
	}
}

// Load đọc YAML config tại path, merge lên Default().
// Field thiếu trong YAML giữ nguyên giá trị Default.
func Load(path string) (GatewayConfig, error) {
	cfg := Default()
	f, err := os.Open(path)
	if err != nil {
		return GatewayConfig{}, fmt.Errorf("config: open %s: %w", path, err)
	}
	defer f.Close()
	if err := yaml.NewDecoder(f).Decode(&cfg); err != nil {
		return GatewayConfig{}, fmt.Errorf("config: decode %s: %w", path, err)
	}
	return cfg, nil
}

// ── Conversion helpers ────────────────────────────────────────────────────────

// ToSessionManagerConfig chuyển đổi sang session.ManagerConfig.
func (c GatewayConfig) ToSessionManagerConfig() session.ManagerConfig {
	return session.ManagerConfig{
		MaxSessions:     c.Session.MaxSessions,
		PacketQueueSize: c.Session.PerSessionPacketQueue,
		AudioQueueSize:  c.Session.PerSessionAudioQueue,
		ResultQueueSize: c.Session.PerSessionResultQueue,
		IdleTimeout:     time.Duration(c.Session.IdleTimeoutSec) * time.Second,
		GCInterval:      time.Duration(c.Session.GCIntervalSec) * time.Second,
	}
}

// ToWorkerPoolConfig chuyển đổi sang pipeline.WorkerPoolConfig.
func (c GatewayConfig) ToWorkerPoolConfig() pipeline.WorkerPoolConfig {
	return pipeline.WorkerPoolConfig{
		Workers:   c.Pipeline.AudioWorkerCount,
		QueueSize: c.Pipeline.AudioJobQueueSize,
	}
}

// ToAIConfig chuyển đổi sang ai.Config.
func (c GatewayConfig) ToAIConfig() ai.Config {
	return ai.Config{
		MaxActiveStreams: c.AI.MaxActiveStreams,
		QueueSize:       c.AI.PerStreamQueueSize,
		SendTimeout:     time.Duration(c.AI.SendTimeoutMs) * time.Millisecond,
		StreamTimeout:   time.Duration(c.AI.StreamTimeoutSec) * time.Second,
		MaxRetries:      c.AI.MaxRetry,
		RetryBackoff:    time.Duration(c.AI.RetryBackoffMs) * time.Millisecond,
	}
}

// ToDispatcherConfig chuyển đổi sang result.Config.
func (c GatewayConfig) ToDispatcherConfig() result.Config {
	return result.Config{
		Workers:             c.Result.DispatcherWorkers,
		QueueSize:           c.Result.QueueSize,
		DropPartialWhenFull: c.Result.DropPartialWhenFull,
		SendTimeout:         time.Duration(c.Result.SendTimeoutMs) * time.Millisecond,
	}
}

// ToCoordinatorConfig chuyển đổi sang coordinator.Config.
func (c GatewayConfig) ToCoordinatorConfig() coordinator.Config {
	return coordinator.Config{
		JitterConfig: jitter.Config{
			BufferMs:     c.Pipeline.JitterBufferMs,
			MaxLateMs:    c.Pipeline.MaxPacketLateMs,
			PacketTimeMs: c.Pipeline.PacketTimeMs,
		},
		OutSampleRate:   c.Audio.OutputSampleRate,
		OutChannels:     c.Audio.OutputChannels,
		ChunkMs:         c.Audio.ChunkMs,
		CallbackTimeout: time.Duration(c.Callback.TimeoutMs) * time.Millisecond,
		CallbackRetry:   c.Callback.MaxRetry,
		PCMDumpDir:      c.Audio.PCMDumpDir,
	}
}

// ToGRPCKeepalive trả về keepalive durations cho SharedConnPool.
func (c GatewayConfig) ToGRPCKeepalive() (keepaliveTime, keepaliveTimeout time.Duration) {
	return time.Duration(c.AI.KeepaliveTimeSec) * time.Second,
		time.Duration(c.AI.KeepaliveTimeoutSec) * time.Second
}

// ToCallbackHTTPClient tạo shared CallbackHTTPClient từ config.
func (c GatewayConfig) ToCallbackHTTPClient() *result.CallbackHTTPClient {
	return result.NewCallbackHTTPClient(
		time.Duration(c.Callback.TimeoutMs)*time.Millisecond,
		time.Duration(c.Callback.ReadIdleTimeoutMs)*time.Millisecond,
		time.Duration(c.Callback.PingTimeoutMs)*time.Millisecond,
	)
}

// ToControlPlaneServerConfig chuyển đổi sang controlplane.ServerConfig.
func (c GatewayConfig) ToControlPlaneServerConfig() controlplane.ServerConfig {
	return controlplane.ServerConfig{
		Addr:         c.Server.HTTPAddr,
		CertFile:     c.Server.CertFile,
		KeyFile:      c.Server.KeyFile,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
		GatewayID:    c.Gateway.ID,
		RTPPublicIP:  c.RTP.PublicIP,
		RTPBindIP:    c.RTP.BindIP,
		RTPPortStart: c.RTP.PortStart,
		RTPPortEnd:   c.RTP.PortEnd,
	}
}

// ToRTPIngressConfig chuyển đổi sang rawrtp.IngressConfig.
func (c GatewayConfig) ToRTPIngressConfig() rawrtp.IngressConfig {
	return rawrtp.IngressConfig{
		Addr:            c.RTP.ListenAddr,
		MaxPacket:       1500,
		ReadBufferBytes: c.RTP.SocketReadBuffer,
	}
}

// ToWebRTCConfig chuyển đổi sang webrtcingress.Config.
func (c GatewayConfig) ToWebRTCConfig() webrtcingress.Config {
	cfg := webrtcingress.Config{
		NAT1To1IPs: c.WebRTC.NAT1To1IPs,
		ICEPortMin: c.WebRTC.ICEPortMin,
		ICEPortMax: c.WebRTC.ICEPortMax,
		DCProxyURL:     c.WebRTC.DCProxyURL,
		DCUDPProxyAddr: c.WebRTC.DCUDPProxyAddr,
	}
	for _, u := range c.WebRTC.STUNServers {
		cfg.ICEServers = append(cfg.ICEServers, webrtcingress.ICEServer{URLs: []string{u}})
	}
	for _, t := range c.WebRTC.TURNServers {
		cfg.ICEServers = append(cfg.ICEServers, webrtcingress.ICEServer{
			URLs:       t.URLs,
			Username:   t.Username,
			Credential: t.Credential,
		})
	}
	return cfg
}
