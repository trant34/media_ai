#include "bridge.h"

#include <cstring>

#include "access_unit_assembler.hpp"
#include "ffmpeg_decoder.hpp"
#include "h264_depacketizer.hpp"
#include "rtp.hpp"

namespace {

// AccessUnitAssembler -> H264Depacketizer -> FFmpegDecoder

class RTPGWDecoderSession {
public:
    RTPGWDecoderSession(int width, int height, const std::string& ffmpeg_bin, bool debug)
        : decoder_(width, height, ffmpeg_bin, debug), width_(width), height_(height) {}

    void push_packet(uint16_t seq, uint32_t ts, uint32_t ssrc, bool marker, uint8_t pt,
                      const uint8_t* payload, int payload_len) {
        decoder::RTPPacket packet;
        packet.seq = seq;
        packet.ts = ts;
        packet.ssrc = ssrc;
        packet.marker = marker;
        packet.pt = pt;
        packet.payload.assign(payload, payload + payload_len);

        for (auto& [rtp_ts, packets] : assembler_.push(packet)) {
            auto annexb = depacketizer_.depacketize(packets);
            if (!annexb.empty()) {
                decoder_.feed(annexb, rtp_ts);
            }
        }
    }

    // Returns 1 + fills out_buf/out_rtp_ts if a frame was available, 0 if
    // not, -1 if out_cap is smaller than this session's frame size.
    int poll_frame(uint8_t* out_buf, int out_cap, uint32_t* out_rtp_ts) {
        size_t needed = static_cast<size_t>(width_) * height_ * 3;
        if (static_cast<size_t>(out_cap) < needed) {
            return -1;
        }
        auto frame = decoder_.frames.get_nowait();
        if (!frame.has_value()) {
            return 0;
        }
        std::memcpy(out_buf, frame->data.data(), needed);
        if (out_rtp_ts) *out_rtp_ts = frame->rtp_ts;
        return 1;
    }

    void close() { decoder_.close(); }

private:
    decoder::AccessUnitAssembler assembler_;
    decoder::H264Depacketizer depacketizer_;
    decoder::FFmpegDecoder decoder_;
    int width_, height_;
};

} // namespace

extern "C" {

rtpgw_decoder_t rtpgw_decoder_create(int width, int height, const char* ffmpeg_bin, int debug) {
    try {
        auto* session = new RTPGWDecoderSession(width, height, ffmpeg_bin ? ffmpeg_bin : "ffmpeg", debug != 0);
        return static_cast<rtpgw_decoder_t>(session);
    } catch (...) {
        return nullptr;
    }
}

void rtpgw_decoder_push_packet(rtpgw_decoder_t handle,
                                uint16_t seq, uint32_t ts, uint32_t ssrc,
                                int marker, uint8_t payload_type,
                                const uint8_t* payload, int payload_len) {
    if (!handle) return;
    static_cast<RTPGWDecoderSession*>(handle)->push_packet(seq, ts, ssrc, marker != 0, payload_type, payload, payload_len);
}

int rtpgw_decoder_poll_frame(rtpgw_decoder_t handle, uint8_t* out_buf, int out_cap, uint32_t* out_rtp_ts) {
    if (!handle) return 0;
    return static_cast<RTPGWDecoderSession*>(handle)->poll_frame(out_buf, out_cap, out_rtp_ts);
}

void rtpgw_decoder_close(rtpgw_decoder_t handle) {
    if (!handle) return;
    static_cast<RTPGWDecoderSession*>(handle)->close();
}

void rtpgw_decoder_destroy(rtpgw_decoder_t handle) {
    if (!handle) return;
    delete static_cast<RTPGWDecoderSession*>(handle);
}

} // extern "C"
