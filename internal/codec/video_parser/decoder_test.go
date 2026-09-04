//go:build ffmpeg

// +build ffmpeg

package video_parser

// decoder_test.go — test độc lập cho H264 decode chain, đúng tinh thần
// internal/codec/*_test.go (evs_test.go, amr_wb_opencore_test.go): hand-craft/generate fixture ngay trong
// test thay vì đọc file .pcap ngoài.
//
// Khác biệt so với G.711/EVS/AMR: H264 không thể hand-craft vài byte hợp lệ
// như TOC byte của AMR — nên thay vì literal byte, test tự dựng fixture bằng
// cách cho `ffmpeg` sinh 1 clip test-pattern ngắn, phát RTP thật qua UDP loopback,
// rồi đọc lại UDP packet đó làm input cho decoder. Đây vẫn là fixture 100%
// tự sinh trong quá trình test (không cần file .pcap commit sẵn), nhưng dùng
// đúng RTP framing/marker-bit/FU-A do ffmpeg tạo ra — sát với traffic thật hơn
// là tự ráp RTP bằng tay.

import (
	"context"
	"net"
	"os/exec"
	"testing"
	"time"

	"github.com/pion/rtp"
)

const (
	testWidth   = 160
	testHeight  = 120
	testFPS     = 10
	testSeconds = 2 // duration=2s @ 10fps ≈ 20 frame input
)

// requireFFmpeg skip test nếu ffmpeg không có trên PATH — mirror cách
// amr_wb_opencore_test.go chỉ chạy khi có libopencore-amrwb (qua build tag),
// nhưng ở đây dùng runtime skip vì dependency là external binary, không phải
// link-time cgo lib.
func requireFFmpeg(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg not found on PATH — skip (cần ffmpeg để sinh fixture H264, xem package doc)")
	}
}

// genH264RTPFixture cho ffmpeg sinh 1 clip testsrc ngắn, encode H264 baseline,
// phát RTP thật qua UDP loopback, đọc lại thành []*rtp.Packet. Trả về sớm nếu
// ffmpeg thoát hoặc hết timeout — không cần biết trước số packet chính xác.
func genH264RTPFixture(t *testing.T, w, h, fps, seconds int) []*rtp.Packet {
	t.Helper()

	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatalf("listen udp: %v", err)
	}
	defer conn.Close()
	port := conn.LocalAddr().(*net.UDPAddr).Port

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "ffmpeg",
		"-hide_banner", "-loglevel", "error",
		"-f", "lavfi", "-i", "testsrc2=duration="+itoa(seconds)+":size="+itoa(w)+"x"+itoa(h)+":rate="+itoa(fps),
		"-c:v", "libx264", "-profile:v", "baseline", "-tune", "zerolatency",
		"-pix_fmt", "yuv420p",
		"-f", "rtp", "rtp://127.0.0.1:"+itoa(port),
	)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start ffmpeg: %v", err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })

	var packets []*rtp.Packet
	buf := make([]byte, 2000)
	deadline := time.Now().Add(10 * time.Second)
	idleLimit := 500 * time.Millisecond // ngừng đọc nếu im lặng 500ms liên tục sau khi đã có ít nhất 1 packet

	for {
		if time.Now().After(deadline) {
			break
		}
		_ = conn.SetReadDeadline(time.Now().Add(idleLimit))
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			if len(packets) > 0 {
				break // hết dữ liệu (ffmpeg đã gửi xong) → coi như đủ fixture
			}
			continue // chưa có packet nào, tiếp tục chờ (ffmpeg đang khởi động/encode)
		}
		pkt := &rtp.Packet{}
		if err := pkt.Unmarshal(buf[:n]); err != nil {
			continue
		}
		packets = append(packets, pkt)
	}

	if len(packets) == 0 {
		t.Fatalf("không nhận được RTP packet nào từ ffmpeg trong thời gian chờ")
	}
	return packets
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

// TestDecoder_H264_Roundtrip là test chính của Giai đoạn 1: feed RTP packet
// H264 thật (sinh bởi ffmpeg) vào decode chain, verify ra đúng số frame bgr24
// đúng kích thước — hoàn toàn độc lập session/pipeline/AI.
func TestDecoder_H264_Roundtrip(t *testing.T) {
	requireFFmpeg(t)

	packets := genH264RTPFixture(t, testWidth, testHeight, testFPS, testSeconds)
	t.Logf("fixture: %d RTP packet từ ffmpeg", len(packets))

	dec, err := NewDecoder(testWidth, testHeight, "ffmpeg")
	if err != nil {
		t.Fatalf("NewDecoder: %v", err)
	}
	defer dec.Close()

	for _, pkt := range packets {
		dec.PushPacket(pkt.SequenceNumber, pkt.Timestamp, pkt.SSRC, pkt.Marker, pkt.PayloadType, pkt.Payload)
	}

	var frames [][]byte
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		frame, _, ok := dec.PollFrame()
		if !ok {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		frames = append(frames, frame)
		if len(frames) >= testFPS*testSeconds {
			break // đã đủ số frame kỳ vọng, không cần đợi hết deadline
		}
	}

	if len(frames) == 0 {
		t.Fatal("không decode được frame nào — kiểm tra ffmpeg binary / AU assembler / depacketizer")
	}

	wantFrameBytes := testWidth * testHeight * 3
	for i, f := range frames {
		if len(f) != wantFrameBytes {
			t.Errorf("frame[%d]: len = %d, want %d (bgr24 %dx%d)", i, len(f), wantFrameBytes, testWidth, testHeight)
		}
	}

	// Cho phép sai số: encoder có thể trễ 1-2 frame đầu (lookahead), và vài
	// frame cuối có thể chưa kịp decode trước deadline.
	minExpected := testFPS*testSeconds - 4
	if len(frames) < minExpected {
		t.Errorf("số frame decode được = %d, kỳ vọng >= %d (input ~%d frame)", len(frames), minExpected, testFPS*testSeconds)
	}
	t.Logf("decode OK: %d/%d frame, mỗi frame %d bytes", len(frames), testFPS*testSeconds, wantFrameBytes)
}

// TestDecoder_LossyPackets mô phỏng packet loss + reorder (RTP thật trên
// network luôn có khả năng này) — AU assembler/depacketizer phải KHÔNG
// panic/deadlock, dù output có thể thiếu vài frame quanh chỗ mất gói.
func TestDecoder_LossyPackets(t *testing.T) {
	requireFFmpeg(t)

	packets := genH264RTPFixture(t, testWidth, testHeight, testFPS, testSeconds)
	if len(packets) < 10 {
		t.Skip("fixture quá ít packet để test lossy/reorder có ý nghĩa")
	}

	// Drop mỗi packet thứ 7, và hoán đổi cặp liền kề mỗi 11 packet — mô phỏng
	// loss ~14% + reorder cục bộ, không đại diện điều kiện mạng thật nhưng đủ
	// để phát hiện assembler bị panic/deadlock trên input bất thường.
	var lossy []*rtp.Packet
	for i, p := range packets {
		if i%7 == 6 {
			continue // drop
		}
		lossy = append(lossy, p)
	}
	for i := 0; i+1 < len(lossy); i += 11 {
		lossy[i], lossy[i+1] = lossy[i+1], lossy[i]
	}

	dec, err := NewDecoder(testWidth, testHeight, "ffmpeg")
	if err != nil {
		t.Fatalf("NewDecoder: %v", err)
	}
	defer dec.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for _, pkt := range lossy {
			dec.PushPacket(pkt.SequenceNumber, pkt.Timestamp, pkt.SSRC, pkt.Marker, pkt.PayloadType, pkt.Payload)
		}
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if _, _, ok := dec.PollFrame(); ok {
				continue
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	select {
	case <-done:
		// OK — không panic/deadlock trong lúc feed input lossy/reorder.
	case <-time.After(20 * time.Second):
		t.Fatal("decoder không thoát trong 20s với input lossy/reorder — nghi ngờ deadlock trong AU assembler")
	}
}
