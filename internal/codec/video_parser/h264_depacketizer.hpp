#pragma once
#include <cstdint>
#include <optional>
#include <vector>

#include "access_unit_assembler.hpp"

namespace decoder {

inline const uint8_t START_CODE[4] = {0x00, 0x00, 0x00, 0x01};

// H264Depacketizer: reassembles single-NALU, STAP-A and
// FU-A RTP payloads into an Annex-B byte stream, re-inserting the last
// seen SPS/PPS ahead of IDR frames that don't carry their own.
class H264Depacketizer {
public:
    std::vector<uint8_t> depacketize(const std::vector<PayloadEntry>& packets) {
        std::vector<std::vector<uint8_t>> nalus;      // NALU storage
        std::vector<uint8_t> fu_buffer;               // buffer for FU-A reassembly
        bool fu_active = false;                       // processing FU-A sequence

        for (const auto& [seq, payload] : packets) {    
            (void)seq;
            if (payload.empty()) continue;
            uint8_t nal_type = payload[0] & 0x1F;
            
            // Handle different NALU types
            if (nal_type >= 1 && nal_type <= 23) {
                nalus.push_back(payload);               // Single NALU
            } else if (nal_type == 24) { // STAP-A
                size_t pos = 1;
                while (pos + 2 <= payload.size()) {
                    uint16_t size = (static_cast<uint16_t>(payload[pos]) << 8) | payload[pos + 1];
                    pos += 2;
                    if (size == 0 || pos + size > payload.size()) break;
                    nalus.emplace_back(payload.begin() + pos, payload.begin() + pos + size);
                    pos += size;
                }
            } else if (nal_type == 28 && payload.size() >= 2) { // FU-A
                uint8_t fu_header = payload[1];
                bool start = (fu_header & 0x80) != 0;
                bool end = (fu_header & 0x40) != 0;
                uint8_t reconstructed_type = fu_header & 0x1F;   // NAL type from FU header
                uint8_t nri = payload[0] & 0x60;                 
                uint8_t forbidden = payload[0] & 0x80;
                // start NALU reconstruction (forbidden + nri + reconstructed_type)
                if (start) {
                    fu_buffer.clear();
                    fu_buffer.push_back(static_cast<uint8_t>(forbidden | nri | reconstructed_type));
                    fu_buffer.insert(fu_buffer.end(), payload.begin() + 2, payload.end());
                    fu_active = true;
                } else if (fu_active) {
                    fu_buffer.insert(fu_buffer.end(), payload.begin() + 2, payload.end());
                }
                // finalize FU-A NALU when end bit is set
                if (end && fu_active) {
                    nalus.push_back(fu_buffer);
                    fu_active = false;
                }
            }
        }

        if (nalus.empty()) {
            return {};
        }

        // Store the last seen SPS and PPS for future IDR frames
        for (const auto& nalu : nalus) {
            uint8_t kind = nalu[0] & 0x1F;
            if (kind == 7) {
                sps_ = nalu;
            } else if (kind == 8) {
                pps_ = nalu;
            }
        }

        // Check if the current access unit contains an IDR frame and whether it has SPS/PPS
        bool has_idr = false, has_sps = false, has_pps = false;
        for (const auto& nalu : nalus) {
            uint8_t kind = nalu[0] & 0x1F;
            has_idr |= (kind == 5);
            has_sps |= (kind == 7);
            has_pps |= (kind == 8);
        }

        std::vector<std::vector<uint8_t>> final_nalus;
        if (has_idr) {
            if (sps_.has_value() && !has_sps) final_nalus.push_back(*sps_);
            if (pps_.has_value() && !has_pps) final_nalus.push_back(*pps_);
        }
        for (auto& nalu : nalus) final_nalus.push_back(std::move(nalu));

        // Construct the final Annex-B byte stream with start codes
        std::vector<uint8_t> out;
        for (const auto& nalu : final_nalus) {
            out.insert(out.end(), START_CODE, START_CODE + 4);
            out.insert(out.end(), nalu.begin(), nalu.end());
        }
        return out;
    }

private:
    std::optional<std::vector<uint8_t>> sps_;
    std::optional<std::vector<uint8_t>> pps_;
};

} // namespace decoder
