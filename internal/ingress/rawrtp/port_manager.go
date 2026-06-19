package rawrtp

import (
	"fmt"
	"net"
	"time"

	pionrtp "github.com/pion/rtp"

	"media-ai-gateway/internal/pipeline"
	"media-ai-gateway/internal/session"
)

// PortReleaser is the interface satisfied by controlplane.PortAllocator.
// Defined here to avoid a circular import between rawrtp and controlplane.
type PortReleaser interface {
	Release(port int)
}

// StartSessionListener binds a dedicated UDP socket to bindIP:port and forwards
// parsed RTP packets to sess.PacketQueue until sess.Ctx is cancelled.
//
// The port is released via releaser.Release when the read goroutine exits.
// On bind failure the error is returned immediately; the caller must release
// the port manually.
func StartSessionListener(
	sess *session.Session,
	bindIP string,
	port int,
	releaser PortReleaser,
) error {
	addr := fmt.Sprintf("%s:%d", bindIP, port)
	conn, err := net.ListenPacket("udp", addr)
	if err != nil {
		return fmt.Errorf("rtp: bind %s: %w", addr, err)
	}
	// Best-effort: set large socket receive buffer to absorb bursts.
	if uc, ok := conn.(*net.UDPConn); ok {
		_ = uc.SetReadBuffer(2 * 1024 * 1024)
	}

	// Unblock ReadFrom when the session ends.
	go func() {
		<-sess.Ctx.Done()
		conn.Close()
	}()

	go func() {
		defer releaser.Release(port)
		defer conn.Close()

		buf := make([]byte, 1500)
		for {
			n, _, err := conn.ReadFrom(buf)
			if err != nil {
				return
			}
			var hdr pionrtp.Header
			offset, err := hdr.Unmarshal(buf[:n])
			if err != nil || offset >= n {
				continue
			}
			pkt := pipeline.MediaPacket{
				SessionID:    sess.ID,
				SourceType:   sess.SourceType,
				SSRC:         hdr.SSRC,
				PayloadType:  hdr.PayloadType,
				Sequence:     hdr.SequenceNumber,
				Timestamp:    hdr.Timestamp,
				Marker:       hdr.Marker,
				Payload:      append([]byte(nil), buf[offset:n]...),
				ReceivedAtMs: time.Now().UnixMilli(),
				Codec:        sess.Codec,
				SampleRate:   sess.SampleRate,
				Channels:     sess.Channels,
			}
			select {
			case sess.PacketQueue <- pkt:
			case <-sess.Ctx.Done():
				return
			default:
				// drop if queue full
			}
		}
	}()

	return nil
}
