#include "ffmpeg_decoder.hpp"

#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <iostream>

#include <fcntl.h>
#include <sys/wait.h>
#include <unistd.h>

namespace decoder {

namespace {
void log(const std::string& message) {
    std::cout << message << std::endl;
}
} // namespace

FFmpegDecoder::FFmpegDecoder(int width, int height, const std::string& ffmpeg_bin, bool debug)
    : width_(width), height_(height), frame_size_(static_cast<size_t>(width) * height * 3) {
    int stdin_pipe[2];
    int stdout_pipe[2];
    if (pipe(stdin_pipe) != 0 || pipe(stdout_pipe) != 0) {
        throw std::runtime_error("failed to create pipes for ffmpeg");
    }

    pid_ = fork();
    if (pid_ < 0) {
        throw std::runtime_error("fork() failed for ffmpeg");
    }

    if (pid_ == 0) {
        // Child: wire up stdin/stdout, optionally silence stderr.
        dup2(stdin_pipe[0], STDIN_FILENO);
        dup2(stdout_pipe[1], STDOUT_FILENO);
        if (!debug) {
            int devnull = open("/dev/null", O_WRONLY);
            if (devnull >= 0) {
                dup2(devnull, STDERR_FILENO);
                ::close(devnull);
            }
        }
        ::close(stdin_pipe[0]);
        ::close(stdin_pipe[1]);
        ::close(stdout_pipe[0]);
        ::close(stdout_pipe[1]);

        std::string loglevel = debug ? "warning" : "error";
        execlp(ffmpeg_bin.c_str(), ffmpeg_bin.c_str(),
               "-loglevel", loglevel.c_str(),
               "-f", "h264", "-probesize", "1M", "-analyzeduration", "1M",
               "-i", "pipe:0", "-an", "-vsync", "0",
               "-f", "rawvideo", "-pix_fmt", "bgr24", "pipe:1",
               static_cast<char*>(nullptr));
        std::perror("execlp ffmpeg failed");
        _exit(127);
    }

    // Parent
    ::close(stdin_pipe[0]);
    ::close(stdout_pipe[1]);
    stdin_fd_ = stdin_pipe[1];
    stdout_fd_ = stdout_pipe[0];

    reader_thread_ = std::thread(&FFmpegDecoder::reader_loop, this);
}

FFmpegDecoder::~FFmpegDecoder() {
    close();
    if (reader_thread_.joinable()) {
        reader_thread_.join();
    }
    if (stdout_fd_ >= 0) ::close(stdout_fd_);
}

void FFmpegDecoder::feed(const std::vector<uint8_t>& annexb, uint32_t rtp_timestamp) {
    if (annexb.empty() || stdin_fd_ < 0) return;
    {
        std::lock_guard<std::mutex> lock(ts_mutex_);
        timestamps_.push_back(rtp_timestamp);
    }
    size_t written = 0;
    while (written < annexb.size()) {
        ssize_t n = write(stdin_fd_, annexb.data() + written, annexb.size() - written);
        if (n < 0) {
            if (errno == EINTR) continue;
            log(std::string("[RTPGW][video_decoder] feed failed: ") + std::strerror(errno));
            return;
        }
        written += static_cast<size_t>(n);
    }
}

void FFmpegDecoder::reader_loop() {
    // ffmpeg must be able to flush buffered frames through this pipe during shutdown,
    // which requires the pipe to keep being drained.
    while (true) {
        ssize_t n = read(stdout_fd_, chunk_buf_.data(), chunk_buf_.size());
        if (n < 0) {
            if (errno == EINTR) continue;
            break;
        }
        if (n == 0) break; // EOF: ffmpeg exited or closed stdout.

        buffer_.insert(buffer_.end(), chunk_buf_.begin(), chunk_buf_.begin() + n);
        while (buffer_.size() >= frame_size_) {
            RawFrame frame;
            frame.data.assign(buffer_.begin(), buffer_.begin() + frame_size_);
            buffer_.erase(buffer_.begin(), buffer_.begin() + frame_size_);

            uint32_t rtp_ts = 0;
            {
                std::lock_guard<std::mutex> lock(ts_mutex_);
                if (!timestamps_.empty()) {
                    rtp_ts = timestamps_.front();
                    timestamps_.pop_front();
                }
            }
            frame.rtp_ts = rtp_ts;
            frames.put_latest(std::move(frame));
        }
    }
}

void FFmpegDecoder::close() {
    bool expected = true;
    if (!running_.compare_exchange_strong(expected, false)) {
        return; // already closed
    }
    if (stdin_fd_ >= 0) {
        ::close(stdin_fd_);
        stdin_fd_ = -1;
    }
    if (pid_ > 0) {
        int status = 0;
        bool exited = false;
        for (int i = 0; i < 30; ++i) { // ~3s
            pid_t result = waitpid(pid_, &status, WNOHANG);
            if (result == pid_) {
                exited = true;
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        if (!exited) {
            kill(pid_, SIGTERM);
            waitpid(pid_, &status, 0);
        }
        pid_ = -1;
    }
    if (reader_thread_.joinable()) {
        reader_thread_.join();
    }
}

} // namespace decoder
