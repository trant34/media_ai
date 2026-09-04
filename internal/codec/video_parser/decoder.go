// Package video decode H264 (RTP) sang PCM ảnh thô (bgr24) qua một bridge C++
// nhúng bằng cgo — port trực tiếp từ prototype mcf (services/logic/internal/video),
// đổi input: thay vì nhận packet forward qua gRPC từ một Media Function (MF)
// riêng, decoder này nhận packet trực tiếp từ Jitter Buffer của chính Gateway
// (xem internal/pipeline/video_pipeline.go).
//
package video_parser

/*
#cgo CXXFLAGS: -std=c++17
#cgo LDFLAGS: -lstdc++
#include <stdlib.h>
#include "bridge.h"
*/
import "C"

import (
	"fmt"
	"sync"
	"unsafe"
)

// Decoder là handle Go-facing cho một phiên decode video (1 session/stream),
// backed bởi C++ bridge. KHÔNG an toàn khi gọi PushPacket/PollFrame đồng thời
// từ nhiều goroutine — theo đúng nguyên tắc "1 stream = 1 owner goroutine"
// dùng trong Video Pipeline Worker Pool.
type Decoder struct {
	handle C.rtpgw_decoder_t
	width  int
	height int

	closeOnce sync.Once
}

// NewDecoder khởi động một tiến trình ffmpeg con (qua C++ bridge) decode ra
// frame bgr24 kích thước width x height. width/height PHẢI khớp với giá trị
// cấu hình inference/decode của session (xem pipeline.PipelineConfig).
func NewDecoder(width, height int, ffmpegBinary string) (*Decoder, error) {
	if ffmpegBinary == "" {
		ffmpegBinary = "ffmpeg"
	}
	cBin := C.CString(ffmpegBinary)
	defer C.free(unsafe.Pointer(cBin))

	handle := C.rtpgw_decoder_create(C.int(width), C.int(height), cBin, C.int(0))
	if handle == nil {
		return nil, fmt.Errorf("codec/video: decoder_create failed (width=%d height=%d ffmpeg=%q)", width, height, ffmpegBinary)
	}
	return &Decoder{handle: handle, width: width, height: height}, nil
}

// PushPacket đẩy field của một RTP packet (H264) vào decode chain
// (AU assembly -> H264 depacketize -> ffmpeg decode).
func (d *Decoder) PushPacket(seq uint16, ts uint32, ssrc uint32, marker bool, payloadType uint8, payload []byte) {
	var payloadPtr *C.uint8_t
	if len(payload) > 0 {
		payloadPtr = (*C.uint8_t)(unsafe.Pointer(&payload[0]))
	}
	markerInt := C.int(0)
	if marker {
		markerInt = C.int(1)
	}
	C.rtpgw_decoder_push_packet(d.handle, C.uint16_t(seq), C.uint32_t(ts), C.uint32_t(ssrc),
		markerInt, C.uint8_t(payloadType), payloadPtr, C.int(len(payload)))
}

// PollFrame kiểm tra non-blocking xem đã có frame bgr24 nào decode xong chưa.
// ok=false nếu chưa có. Gọi trong vòng lặp poll ngắn (xem video_pipeline.go) —
// KHÔNG dùng blocking read vì PollFrame băng qua ranh giới cgo.
func (d *Decoder) PollFrame() (frame []byte, rtpTimestamp uint32, ok bool) {
	buf := make([]byte, d.width*d.height*3)
	var cRTS C.uint32_t
	ret := C.rtpgw_decoder_poll_frame(d.handle, (*C.uint8_t)(unsafe.Pointer(&buf[0])), C.int(len(buf)), &cRTS)
	if ret != 1 {
		return nil, 0, false
	}
	return buf, uint32(cRTS), true
}

// Close dừng tiến trình ffmpeg con. An toàn khi gọi nhiều lần.
func (d *Decoder) Close() {
	d.closeOnce.Do(func() {
		C.rtpgw_decoder_close(d.handle)
		C.rtpgw_decoder_destroy(d.handle)
	})
}
