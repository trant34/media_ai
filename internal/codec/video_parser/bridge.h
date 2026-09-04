#ifndef MCF_CDECODER_BRIDGE_H
#define MCF_CDECODER_BRIDGE_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// rtpgw_decoder_* : C-ABI wrapper around AccessUnitAssembler +
// H264Depacketizer + FFmpegDecoder (see bridge.cpp), so RTPGW (Go) can
// directly decode via cgo.
//
// One handle = one video stream's decode session (mirrors mf_cpp's
// per-process decode state, just now scoped per (session_id, stream_id)
// inside RTPGW instead of per-process).

typedef void* rtpgw_decoder_t;

// width/height: the resolution ffmpeg is told to decode+scale to via its rawvideo/bgr24 output
rtpgw_decoder_t rtpgw_decoder_create(int width, int height, const char* ffmpeg_bin, int debug);

// Pushes one raw RTP packet's fields into the assembler/depacketizer/
// decoder chain. Non-blocking (writes to ffmpeg's stdin pipe may block
// briefly under backpressure -- see bridge.cpp).
void rtpgw_decoder_push_packet(rtpgw_decoder_t handle,
                                uint16_t seq, uint32_t ts, uint32_t ssrc,
                                int marker, uint8_t payload_type,
                                const uint8_t* payload, int payload_len);

// Non-blocking poll for one decoded frame. out_buf must be at least
// width*height*3 bytes (bgr24). Returns 1 and fills out_buf/out_rtp_ts if a
// frame was available, 0 if the queue was empty. Returns -1 if out_cap is
// too small for the configured frame size (caller bug).
int rtpgw_decoder_poll_frame(rtpgw_decoder_t handle, uint8_t* out_buf, int out_cap, uint32_t* out_rtp_ts);

// Closes the underlying ffmpeg subprocess (blocks briefly for it to exit,
// same shutdown sequence as mf_cpp's original FFmpegDecoder::close()).
void rtpgw_decoder_close(rtpgw_decoder_t handle);

// Frees the handle. Call close() first (destroy() does NOT imply close()).
void rtpgw_decoder_destroy(rtpgw_decoder_t handle);

#ifdef __cplusplus
}
#endif

#endif // MCF_CDECODER_BRIDGE_H
