#pragma once
#include <atomic>
#include <cstdint>
#include <cstring>
#include <deque>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "latest_queue.hpp"

namespace decoder {

struct RawFrame {
    uint32_t rtp_ts;
    std::vector<uint8_t> data; // bgr24, width * height * 3 bytes, contiguous
};

// FFmpegDecoder: pipes Annex-B H264 into an `ffmpeg` subprocess and reads
// raw BGR24 frames back out on a background thread.
class FFmpegDecoder {
public:
    FFmpegDecoder(int width, int height, const std::string& ffmpeg_bin, bool debug);
    ~FFmpegDecoder();

    void feed(const std::vector<uint8_t>& annexb, uint32_t rtp_timestamp);
    void close();

    LatestQueue<RawFrame> frames{8};

private:
    void reader_loop();

    int width_;
    int height_;
    size_t frame_size_;
    std::atomic<bool> running_{true};

    pid_t pid_ = -1;
    int stdin_fd_ = -1;
    int stdout_fd_ = -1;

    std::mutex ts_mutex_;
    std::deque<uint32_t> timestamps_;

    std::vector<uint8_t> buffer_;
    std::vector<uint8_t> chunk_buf_ = std::vector<uint8_t>(64 * 1024);
    std::thread reader_thread_;
};

} // namespace decoder
