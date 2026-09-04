#pragma once
#include <cstdint>
#include <cstring>
#include <optional>
#include <vector>

namespace decoder {

// RTPPacket dataclass.
struct RTPPacket {
    uint16_t seq = 0;       // packet sequence number
    uint32_t ts = 0;        // rtp timestamp
    uint32_t ssrc = 0;
    bool marker = false;    // mark the last packet of a frame
    uint8_t pt = 0;         // payload type
    std::vector<uint8_t> payload;    // payload h264 data
};

constexpr uint32_t SEQ_MOD = 65536;

// parse_rtp(). parse UDP packet -> RTP packet Returns nullopt on malformed packet.
inline std::optional<RTPPacket> parse_rtp(const uint8_t* data, size_t len) {
    if (len < 12 || (data[0] >> 6) != 2) {
        return std::nullopt;
    }
    uint8_t cc = data[0] & 0x0F;
    bool has_extension = ((data[0] >> 4) & 1) != 0;
    bool marker = ((data[1] >> 7) & 1) != 0;
    uint8_t payload_type = data[1] & 0x7F;

    uint16_t seq = (static_cast<uint16_t>(data[2]) << 8) | data[3];
    uint32_t timestamp = (static_cast<uint32_t>(data[4]) << 24) |
                          (static_cast<uint32_t>(data[5]) << 16) |
                          (static_cast<uint32_t>(data[6]) << 8) |
                          static_cast<uint32_t>(data[7]);
    uint32_t ssrc = (static_cast<uint32_t>(data[8]) << 24) |
                     (static_cast<uint32_t>(data[9]) << 16) |
                     (static_cast<uint32_t>(data[10]) << 8) |
                     static_cast<uint32_t>(data[11]);

    size_t offset = 12 + static_cast<size_t>(cc) * 4;
    if (offset > len) {
        return std::nullopt;
    }
    if (has_extension) {
        if (offset + 4 > len) {
            return std::nullopt;
        }
        uint16_t extension_words = (static_cast<uint16_t>(data[offset + 2]) << 8) | data[offset + 3];
        offset += 4 + static_cast<size_t>(extension_words) * 4;
        if (offset > len) {
            return std::nullopt;
        }
    }

    RTPPacket packet;
    packet.seq = seq;
    packet.ts = timestamp;
    packet.ssrc = ssrc;
    packet.marker = marker;
    packet.pt = payload_type;
    packet.payload.assign(data + offset, data + len);
    return packet;
}

} // namespace decoder
