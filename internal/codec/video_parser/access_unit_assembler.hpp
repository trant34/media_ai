#pragma once
#include <algorithm>
#include <cstdint>
#include <optional>
#include <vector>

#include "rtp.hpp"

namespace decoder {

using PayloadEntry = std::pair<uint16_t, std::vector<uint8_t>>;    // (seq number, payload)
using AccessUnit = std::pair<uint32_t, std::vector<PayloadEntry>>; // (rtp_ts, packets)

// Groups RTP packets that share the same RTP timestamp into an access unit,
//  flushed on timestamp change or marker bit.

class AccessUnitAssembler {
public:
    // Returns zero or more completed access units produced by this push.
    std::vector<AccessUnit> push(const RTPPacket& packet) {
        std::vector<AccessUnit> output;
        if (!timestamp_.has_value()) {
            timestamp_ = packet.ts;
        }
        // timestamp change -> flush current access unit with the same timestamp
        if (packet.ts != *timestamp_) {
            if (!packets_.empty()) {
                output.emplace_back(*timestamp_, sorted(packets_));
            }
            timestamp_ = packet.ts;
            packets_.clear();
        }
        packets_.emplace_back(packet.seq, packet.payload);

        // marker bit -> flush current access unit with the end signal
        if (packet.marker) {
            output.emplace_back(packet.ts, sorted(packets_));
            timestamp_.reset();
            packets_.clear();
        }
        return output;
    }

private:
    static std::vector<PayloadEntry> sorted(std::vector<PayloadEntry> packets) {
        uint16_t base = packets.empty() ? 0 : packets.front().first;
        std::stable_sort(packets.begin(), packets.end(),
                          [base](const PayloadEntry& a, const PayloadEntry& b) {
                              uint32_t da = (static_cast<uint32_t>(a.first) - base + SEQ_MOD) % SEQ_MOD;
                              uint32_t db = (static_cast<uint32_t>(b.first) - base + SEQ_MOD) % SEQ_MOD;
                              return da < db;
                          });
        return packets;
    }

    std::optional<uint32_t> timestamp_;
    std::vector<PayloadEntry> packets_;
};

} // namespace decoder
