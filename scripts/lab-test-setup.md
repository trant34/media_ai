# Hướng dẫn thiết lập test lab với mock-ai-worker

## Tổng quan

### Test cơ bản (lab-rtp-test.sh)

```
[lab-rtp-test.sh]
      │  HTTP/2  POST /v1/sessions
      ▼
[media-ai-gateway :8080]
      │  per-session UDP  :40000–40099
      │  gRPC protobuf  →  mock-ai-worker :50051
      ▼
[mock-ai-worker :50051]
      └─ partial mỗi 3 AudioChunk (~1.5s), final mỗi 6 AudioChunk (~3s)
```

### Test callback E2E (test-rtp-callback.sh)

```
[test-rtp-callback.sh]
      │  POST /v1/sessions  (+ callback_url)
      │  UDP RTP packets
      ▼
[media-ai-gateway :8080]
      │  gRPC  →  mock-ai-worker :50051
      │  HTTP/2 POST callback
      ▼
[mock-callback-server :9999]   ← Go binary, built-in H2C server
      └─ log {"event":"callback",...} ra stdout
```

**Pipeline audio (PCMU 8kHz → 16kHz):**
- 1 RTP packet = 160 bytes = 20ms audio
- 1 AudioChunk (ChunkMs=500) = 25 RTP packets
- Mock gửi partial lần đầu sau **75 packets**, final sau **150 packets**
- Script gửi **200 packets** → 8 AudioChunk → 2 partial + 1 final

---

## Bước 1 — Build binary

### Option A: Cross-compile trên Windows (PowerShell)

```powershell
# Chạy tại D:\NAMCHT\ims\SRC\media_ai
New-Item -ItemType Directory -Force bin | Out-Null

$env:GOOS="linux"; $env:GOARCH="amd64"
go build -mod=vendor -o bin/media-ai-gateway    ./cmd/media-ai-gateway
go build -mod=vendor -o bin/mock-ai-worker      ./cmd/mock-ai-worker
go build -mod=vendor -o bin/mock-callback-server ./cmd/mock-callback-server
Remove-Item Env:\GOOS, Env:\GOARCH
```

### Option B: Build trực tiếp trên lab Linux

```bash
# Yêu cầu Go >= 1.22 đã cài trên lab
cd ~/media-ai/src
go build -mod=vendor -o ../bin/media-ai-gateway    ./cmd/media-ai-gateway
go build -mod=vendor -o ../bin/mock-ai-worker      ./cmd/mock-ai-worker
go build -mod=vendor -o ../bin/mock-callback-server ./cmd/mock-callback-server
```

---c

## Bước 2 — Copy lên lab server

```bash
# Chạy từ máy Windows (điều chỉnh USER và LAB_IP)
LAB="USER@LAB_IP"
scp bin/media-ai-gateway         $LAB:~/media-ai/
scp bin/mock-ai-worker           $LAB:~/media-ai/
scp bin/mock-callback-server     $LAB:~/media-ai/
scp config/gateway-mock.yaml     $LAB:~/media-ai/
scp scripts/lab-rtp-test.sh      $LAB:~/media-ai/
scp scripts/test-rtp-callback.sh $LAB:~/media-ai/
scp scripts/test-callback-e2e.sh $LAB:~/media-ai/
scp scripts/test-pcm-real-ai.sh  $LAB:~/media-ai/
scp scripts/test-real-ai-g711.sh $LAB:~/media-ai/
```

---

## Bước 3 — Cấu hình (chỉ khi gửi RTP từ máy khác)

Nếu máy chạy `lab-rtp-test.sh` khác với lab server, sửa `gateway-mock.yaml`:

```yaml
rtp:
  public_ip: "192.168.x.x"   # thay bằng IP thực của lab server
```

Mặc định `public_ip: "127.0.0.1"` dùng khi test trên cùng máy.

---

## Bước 4 — Khởi động (3 terminal hoặc tmux)

### Terminal 1 — mock-ai-worker

```bash
cd ~/media-ai
chmod +x mock-ai-worker
./mock-ai-worker --addr :50051 --log-level debug
```

Chờ log:
```
{"msg":"mock-ai-worker listening","addr":":50051"}
```

### Terminal 2 — gateway

```bash
cd ~/media-ai
chmod +x media-ai-gateway
./media-ai-gateway --config gateway-mock.yaml 2>&1 | tee /tmp/gw.log
```

Chờ log:
```
{"msg":"ai: gRPC connection initiated","addr":"127.0.0.1:50051","state":"CONNECTING"}
{"msg":"AI routing dialer active","target":"127.0.0.1:50051","keepalive_time":"30s","keepalive_timeout":"10s"}
{"msg":"callback: preconnected via HTTP/2","url":"http://127.0.0.1:9999/","status":200}
{"msg":"HTTP/2 control plane listening","addr":":8080","mode":"h2c"}
{"msg":"RTP ingress listening","addr":":5004"}
```

> **AI gRPC pre-connect**: `"ai: gRPC connection initiated"` luôn xuất hiện khi `ai.grpc_target` được cấu hình. State ban đầu là `CONNECTING` — gRPC tự động chuyển sang `READY` sau khi TCP handshake thành công với mock-ai-worker. Nếu mock-ai-worker chưa chạy, gateway vẫn khởi động bình thường và gRPC retry khi có session đầu tiên.

> **Callback pre-connect**: Log `callback: preconnected via HTTP/2` chỉ xuất hiện khi `callback.url` được cấu hình **và** `mock-callback-server` đang chạy. Nếu mock-callback-server chưa start thì log là `callback: preconnect failed — will retry on first callback` — không ảnh hưởng khởi động.

### Terminal 3 — lab test script

```bash
cd ~/media-ai
chmod +x lab-rtp-test.sh

# Test trên cùng máy (mặc định)
./lab-rtp-test.sh

# Test từ xa (gateway_host gateway_port)
./lab-rtp-test.sh 192.168.x.x 8080
```

---

## Bước 5 — Kết quả kỳ vọng

### Terminal 3 (lab-rtp-test.sh)

```
[OK]  Gateway ready
[OK]  Session created (HTTP 201, HTTP/2.0)
[OK]  Per-session RTP endpoint: 127.0.0.1:40000
      Sent 200/200 packets...
[OK]  Audio jobs submitted: +200 ✓
[OK]  Audio jobs processed: +200 ✓
[OK]  Session still active ✓
[OK]  AI gRPC stream active ✓
[OK]  AI send errors: 0 ✓
[OK]  AI recv errors: 0 ✓
[OK]  Session found (HTTP 200)
[OK]  Session deleted (HTTP 204)
[OK]  Port returned to pool ✓
[OK]  Lab test PASSED
```

### Terminal 1 (mock-ai-worker) — trong quá trình test

```json
{"msg":"stream opened","session_id":"lab-rtp-...","language":"vi","task":"transcribe"}
{"msg":"sent partial","seq":1,"text":"xin chào..."}
{"msg":"sent final","seq":3,"text":"Cuộc gọi đến từ khách hàng.","chunks":6}
{"msg":"sent partial","seq":4,"text":"đây là kết quả..."}
{"msg":"stream closed (client EOF)","total_chunks":8,"total_seq":4}
```

---

## Bước 6 — Callback E2E Test (test-rtp-callback.sh)

Kiểm tra toàn bộ pipeline bao gồm chiều trả về: gateway gửi kết quả ASR về client qua HTTP/2 POST.

### Yêu cầu

- `mock-callback-server` đã build (Bước 1)
- `media-ai-gateway` và `mock-ai-worker` đang chạy (Bước 4)

### Chạy

```bash
cd ~/media-ai
chmod +x test-rtp-callback.sh mock-callback-server

# Mặc định: cổng 9999, chờ 1 final result, timeout 30s
./test-rtp-callback.sh

# Tùy chỉnh
CALLBACK_PORT=9999 EXPECT_FINAL=1 CALLBACK_TIMEOUT=30 ./test-rtp-callback.sh 127.0.0.1 8080
```

Script tự động:
1. Tìm hoặc build `mock-callback-server` (nếu chưa có trong cùng thư mục)
2. Khởi động `mock-callback-server --port 9999 --expect-final 1 --timeout 30s`
3. Tạo session với `callback_url: http://127.0.0.1:9999`
4. Gửi 200 PCMU RTP packets
5. Chờ gateway gửi callback HTTP/2 POST tới mock server
6. Verify kết quả

### Kết quả kỳ vọng

```
[OK]  mock-callback-server ready ✓
[OK]  Session created (HTTP 201, HTTP/2.0) ✓
[OK]  Callback(s) nhận thành công sau Xs ✓
[OK]  Nhận 1 final callback (expect ≥1) ✓
[OK]  Gateway đã gửi callback thành công: +1 ✓
[OK]  Không có callback error ✓
[OK]  Pipeline decode OK: +200/200 jobs ✓
[OK]  Callback E2E test PASSED ✓
```

### Output mock-callback-server

Mỗi dòng là một JSON:

```json
{"event":"ready","port":9999}
{"event":"callback","event_type":"asr.transcript.partial","session_id":"cb-...","text":"xin","is_final":false,"seq":1}
{"event":"callback","event_type":"asr.transcript.final","session_id":"cb-...","text":"xin chào","is_final":true,"seq":2}
{"event":"summary","stats":{"asr.transcript.final":1,"asr.transcript.partial":2}}
```

### PATCH session — cập nhật callback_url và IMS identity (tùy chọn)

Sau khi tạo session, gửi PATCH để cập nhật callback URL và/hoặc thêm IMS identity:

```bash
SESSION_ID="cb-..."   # lấy từ response POST /v1/sessions

# Cập nhật callback_url + contextId/terminationId cùng lúc
curl -s --http2-prior-knowledge \
  -X PATCH http://127.0.0.1:8080/v1/sessions/$SESSION_ID \
  -H "Content-Type: application/json" \
  -d '{
    "callback_url": "http://127.0.0.1:9999",
    "mediaResources": {
      "tAccess": {
        "contextId":     "ctx-test",
        "terminationId": "term-test",
        "endpoint":      "127.0.0.1:5004"
      }
    }
  }'
```

Tác động:
- `callback_url` → thay callback sink ngay lập tức (kết quả tiếp theo gửi đến URL mới)
- `tAccess.endpoint` → cập nhật remote RTP addr cho session
- `tAccess.contextId` / `terminationId` → xuất hiện ở top-level trong tất cả callback kể từ lúc PATCH

Các callback tiếp theo sẽ có thêm:
```json
{ ..., "contextId": "ctx-test", "terminationId": "term-test" }
```

---

## Bước 7 — AMR-WB PCM Dump Test (test-pcm-dump-amrwb.sh)

Kiểm tra pipeline decode AMR-WB thực sự ra PCM int16-LE và ghi xuống file.
Script **tự quản lý toàn bộ**: build CGO binary → start services → gửi RTP → verify output → cleanup.

### Kiến trúc

```
[test-pcm-dump-amrwb.sh]
      │  build  →  gateway-amrwb (CGO + opencore_amrwb tag)
      │  build  →  mock-rtp-sender
      │
      │  start  →  mock-ai-worker :50051
      │  start  →  gateway-amrwb  :8080  (pcm_dump_dir=data/output/pcm)
      │
      │  POST /v1/sessions  (codec=AMR-WB)
      │
      ▼
[mock-rtp-sender]
      │  UDP 200 packet × 62 bytes  →  gateway-amrwb :40xxx
      │  Format: CMR(0xF0) + TOC(0x44) + 60 bytes AMR-WB FT=8 frame
      ▼
[gateway-amrwb]
      │  opencore-amrwb decode  →  PCM int16-LE 16kHz
      │  ghi mỗi frame vào: data/output/pcm/<session>.amrwb.16000hz.1ch.s16le
      │  resample + chunk → AudioChunk → mock-ai-worker
      ▼
[data/output/pcm/<session>.amrwb.16000hz.1ch.s16le]
      └─  ~128,000 bytes  (200 frames × 320 samples × 2 bytes)
```

### Yêu cầu

| Thành phần | Lệnh cài |
|---|---|
| Linux (Ubuntu/Debian) | — |
| opencore-amrwb | `apt-get install libopencore-amrwb-dev` |
| Go ≥ 1.22 với CGO | mặc định bật trên Linux |
| curl, python3 | thường đã có sẵn |

> **Không chạy được trên Windows** — CGO yêu cầu Linux toolchain và `libopencore-amrwb.so`.

### Bước 1 — Copy source lên lab server

Từ máy Windows, copy toàn bộ project (hoặc chỉ những file cần thiết):

```bash
LAB="USER@LAB_IP"

# Toàn bộ source (nếu chưa có)
scp -r D:/NAMCHT/ims/SRC/media_ai  $LAB:~/media-ai/src

# Hoặc chỉ update các file mới nhất
scp D:/NAMCHT/ims/SRC/media_ai/cmd/mock-rtp-sender/main.go      $LAB:~/media-ai/src/cmd/mock-rtp-sender/
scp D:/NAMCHT/ims/SRC/media_ai/internal/pipeline/audio_pipeline.go  $LAB:~/media-ai/src/internal/pipeline/
scp D:/NAMCHT/ims/SRC/media_ai/internal/coordinator/coordinator.go   $LAB:~/media-ai/src/internal/coordinator/
scp D:/NAMCHT/ims/SRC/media_ai/internal/config/config.go             $LAB:~/media-ai/src/internal/config/
scp D:/NAMCHT/ims/SRC/media_ai/config/gateway-mock.yaml              $LAB:~/media-ai/src/config/
scp D:/NAMCHT/ims/SRC/media_ai/scripts/test-pcm-dump-amrwb.sh        $LAB:~/media-ai/src/scripts/
scp D:/NAMCHT/ims/SRC/media_ai/data/generated/amrwb/speech.amr       $LAB:~/media-ai/src/data/generated/amrwb/
```

### Bước 2 — Cài thư viện trên lab server

```bash
# Ubuntu / Debian
sudo apt-get update && sudo apt-get install -y libopencore-amrwb-dev

# RHEL / CentOS (cần EPEL)
sudo yum install -y epel-release && sudo yum install -y opencore-amr-devel

# Verify
ldconfig -p | grep libopencore-amrwb
# → libopencor-amrwb.so.0 (libc6,x86-64) => /usr/lib/x86_64-linux-gnu/libopencore-amrwb.so.0
```

### Bước 3 — Chạy test (tự động hoàn toàn)

```bash
cd ~/media-ai/src
chmod +x scripts/test-pcm-dump-amrwb.sh

# Chạy trên cùng máy (mặc định)
./scripts/test-pcm-dump-amrwb.sh

# Chỉ định host/port nếu cần
./scripts/test-pcm-dump-amrwb.sh 127.0.0.1 8080
```

Script **không cần khởi động gateway/worker trước** — nó tự build và quản lý mọi process.
Nếu port 8080 hoặc 50051 đang dùng, script sẽ kill process cũ trước khi start.

### Bước 4 — Kết quả kỳ vọng

```
╔═══════════════════════════════════════════════════╗
║  AMR-WB PCM Dump Test (CGO / opencore-amrwb)     ║
╚═══════════════════════════════════════════════════╝

[INFO]  Step 1 — Preflight
[OK]    libopencore-amrwb found ✓
[OK]    go go1.22.x ✓
[OK]    AMR-WB file: .../speech.amr (183070 bytes, ~3001 frames FT=8) ✓
[OK]    gateway-mock.yaml có pcm_dump_dir ✓

[INFO]  Step 2 — Build binaries với opencore_amrwb tag
[OK]      gateway-amrwb built ✓
[OK]      mock-ai-worker built ✓
[OK]      mock-rtp-sender built ✓

[INFO]  Step 3 — Khởi động services
[OK]    Gateway ready (HTTP 200) ✓

[INFO]  Step 4 — Tạo AMR-WB session
[OK]    Session created (HTTP 201)
[OK]    RTP endpoint: 127.0.0.1:40099

[INFO]  Step 6 — Gửi 200 AMR-WB packets qua mock-rtp-sender
        file_format=amrwb  pt=98  ssrc=60099  ptime_ms=20  ts_incr=320
        sent=50  sent=100  sent=150  sent=200
        packets_sent=200  frames_skipped=0

[INFO]  Step 8 — Kiểm tra PCM dump file
[OK]    PCM file tồn tại ✓
[OK]    PCM file không trống ✓
[OK]    PCM file size hợp lệ: 128000 bytes (expect 121600–134400) ✓
        Tổng samples   : 64,000  (4.00s @ 16kHz)
        Non-zero samples: 63,412 (99.1%)
        Max amplitude  : 18432 / 32767  (56.3% of full scale)
        RMS amplitude  : 2814.3
        [OK] PCM có tín hiệu audio hợp lệ ✓
        Duration       : 4.00s  (expect ~4.00s)

[INFO]  Step 9 — Kiểm tra metrics
[OK]      Submitted: +200/200 ✓
[OK]      Processed: +200/200 ✓
[OK]      Decode errors: 0 — AMR-WB CGO decode thành công ✓

[INFO]  Step 10 — Playback
        ffplay -f s16le -ar 16000 -ac 1 'data/output/pcm/amrwb-pcmdump-xxx.amrwb.16000hz.1ch.s16le'

══════════════ AMR-WB PCM Dump Test Summary ══════════════
  RESULT: PASS
  AMR-WB → PCM decode + dump hoạt động đúng ✓
```

### Bước 5 — Nghe / kiểm tra PCM output

```bash
# Phát trực tiếp (cần ffplay / ffmpeg)
ffplay -f s16le -ar 16000 -ac 1 \
    data/output/pcm/amrwb-pcmdump-*.amrwb.16000hz.1ch.s16le

# Chuyển sang WAV để nghe trên mọi player
sox -r 16000 -e signed -b 16 -c 1 \
    data/output/pcm/amrwb-pcmdump-*.amrwb.16000hz.1ch.s16le \
    /tmp/output.wav

# Xem waveform (nếu có sox)
sox data/output/pcm/*.s16le -n \
    -r 16000 -e signed -b 16 -c 1 \
    stat
```

### Troubleshooting — AMR-WB PCM Dump

| Triệu chứng | Nguyên nhân | Cách fix |
|---|---|---|
| `libopencore-amrwb không tìm thấy` | Chưa cài thư viện | `apt-get install libopencore-amrwb-dev` |
| `Build gateway thất bại` | CGO_ENABLED=0 hoặc thiếu header | `export CGO_ENABLED=1` rồi kiểm tra `apt list --installed \| grep amrwb` |
| PCM file **trống** (0 bytes) | Binary không link amrwb | `ldd bin/gateway-amrwb \| grep amrwb` — phải thấy `libopencore-amrwb.so` |
| `Decode errors: +200` | Binary là stub (no CGO) | Dùng đúng binary: `bin/media-ai-gateway-amrwb`, không phải `bin/media-ai-gateway` |
| `Gateway not ready (HTTP 000)` | Port 8080 bị chặn firewall | `sudo ufw allow 8080` hoặc chạy trên loopback |
| PCM có data nhưng **toàn 0** | opencore decode silent | Kiểm tra AMR file hợp lệ: `xxd speech.amr \| head -1` phải thấy `#!AMR-WB` |
| `PCM size quá nhỏ` | Jitter buffer drop nhiều packet | Kiểm tra UDP không bị mất gói: `ss -s` khi chạy test |
| `Non-zero < 10%` | File AMR là silence | Thay bằng file có tiếng nói thực |

---

## Bước 8 — Chạy với Docker Desktop (Windows)

Cách nhanh nhất để có môi trường Linux + CGO + opencore-amrwb mà **không cần lab server**:
Docker Desktop cung cấp Linux container trực tiếp trên Windows.

### Kiến trúc Docker

```
Windows Host (Docker Desktop)
│
└─ Linux Container (golang:latest + libopencore-amrwb-dev)
       │  mount  D:\NAMCHT\ims\SRC\media_ai  →  /app
       │
       ├─ bash scripts/test-pcm-dump-amrwb.sh
       │     ├─ go build -tags opencore_amrwb  →  /app/bin/media-ai-gateway-amrwb
       │     ├─ /app/bin/mock-ai-worker   :50051  (trong container)
       │     ├─ /app/bin/media-ai-gateway :8080   (trong container)
       │     ├─ /app/bin/mock-rtp-sender  → UDP :40xxx
       │     └─ PCM output: /app/data/output/pcm/*.s16le
       │
       └─ /app/data/output/pcm/*.s16le
              ↕  (mount, truy cập trực tiếp từ Windows)
         D:\NAMCHT\ims\SRC\media_ai\data\output\pcm\*.s16le
```

### Yêu cầu

- Docker Desktop đang chạy (kiểm tra: `docker --version`)
- Không cần cài Go, libopencore-amrwb, hay bất kỳ thứ gì khác trên Windows

### Bước 1 — Build Docker image (1 lần)

```powershell
# Chạy tại D:\NAMCHT\ims\SRC\media_ai
cd D:\NAMCHT\ims\SRC\media_ai

docker build -f docker/Dockerfile.test-amrwb -t media-ai-test-amrwb .
```

Lần đầu mất 3–5 phút (pull `golang:latest` + install packages).
Các lần sau dùng cache, gần như tức thì.

Kiểm tra image đã build:
```powershell
docker image ls media-ai-test-amrwb
```

### Bước 2 — Chạy test

```powershell
# Chạy tại D:\NAMCHT\ims\SRC\media_ai
docker run --rm -v "${PWD}:/app" media-ai-test-amrwb
```

> **Lưu ý:** `${PWD}` trong PowerShell là thư mục hiện tại.
> Nếu dùng Command Prompt, thay bằng `%CD%`.

Container sẽ:
1. Build gateway với CGO + opencore_amrwb
2. Khởi động mock-ai-worker và gateway bên trong container
3. Gửi 200 packet AMR-WB
4. Kiểm tra PCM output
5. Tự dừng và cleanup

### Bước 3 — Xem kết quả PCM

Sau khi container kết thúc, file PCM xuất hiện trực tiếp trên Windows:

```powershell
# Liệt kê file PCM output
ls D:\NAMCHT\ims\SRC\media_ai\data\output\pcm\

# Kiểm tra kích thước (phải ~128,000 bytes)
(Get-Item "D:\NAMCHT\ims\SRC\media_ai\data\output\pcm\*.s16le").Length
```

Để **nghe audio** trên Windows (cần cài [ffmpeg](https://ffmpeg.org/download.html)):

```powershell
# Tìm file PCM vừa tạo
$pcm = (Get-ChildItem "D:\NAMCHT\ims\SRC\media_ai\data\output\pcm\*.s16le" | Sort-Object LastWriteTime -Descending | Select-Object -First 1).FullName

# Chuyển sang WAV và phát
ffmpeg -f s16le -ar 16000 -ac 1 -i $pcm /tmp/output.wav
# Hoặc phát trực tiếp
ffplay -f s16le -ar 16000 -ac 1 $pcm
```

### Kết quả kỳ vọng

```
Step 1 — Preflight
[OK]    libopencore-amrwb found ✓
[OK]    go go1.25.x linux/amd64 ✓

Step 2 — Build binaries với opencore_amrwb tag
[OK]      gateway-amrwb built ✓
[OK]      mock-ai-worker built ✓
[OK]      mock-rtp-sender built ✓

Step 3 — Khởi động services
[OK]    Gateway ready (HTTP 200) ✓

Step 6 — Gửi 200 AMR-WB packets qua mock-rtp-sender
        packets_sent=200  frames_skipped=0

Step 8 — Kiểm tra PCM dump file
[OK]    PCM file size hợp lệ: 128000 bytes ✓
        Tổng samples   : 64,000  (4.00s @ 16kHz)
        Non-zero samples: 63,412 (99.1%)
[OK]    PCM có tín hiệu audio hợp lệ ✓

Step 9 — Kiểm tra metrics
[OK]      Decode errors: 0 — AMR-WB CGO decode thành công ✓

  RESULT: PASS
```

### Các lệnh hữu ích

```powershell
# Xem log real-time trong quá trình test
docker run --rm -v "${PWD}:/app" media-ai-test-amrwb 2>&1 | Tee-Object -Variable output

# Vào container để debug thủ công (không chạy test)
docker run --rm -it -v "${PWD}:/app" media-ai-test-amrwb bash

# Rebuild image sau khi sửa Dockerfile
docker build --no-cache -f docker/Dockerfile.test-amrwb -t media-ai-test-amrwb .

# Xóa image khi không cần nữa
docker image rm media-ai-test-amrwb
```

### Troubleshooting — Docker

| Triệu chứng | Nguyên nhân | Cách fix |
|---|---|---|
| `docker: command not found` | Docker Desktop chưa start | Mở Docker Desktop, chờ biểu tượng ổn định |
| `invalid reference format` | `${PWD}` không được hỗ trợ | Dùng đường dẫn tuyệt đối: `-v "D:/NAMCHT/ims/SRC/media_ai:/app"` |
| Build image timeout | Mạng chậm khi pull base image | Chạy lại; hoặc dùng `--network host` |
| `permission denied: /app/bin/` | Mount volume read-only | Không thêm `:ro` vào lệnh `-v` |
| `go: toolchain go1.25.0 unavailable` | `golang:latest` chưa có Go 1.25 | Đã set `GOTOOLCHAIN=local` — tự build với Go hiện tại |
| Container thoát ngay không có log | Script lỗi sớm | `docker run --rm -it -v "${PWD}:/app" media-ai-test-amrwb bash` để debug |
| PCM file không xuất hiện trên Windows | Test bị fail trước step 8 | Xem log, tìm `[FAIL]` |

---

## Bước 9 — Test PCM với AI thật (test-pcm-real-ai.sh)

Full end-to-end test: file audio thật (60s PCMU G.711), gateway kết nối gRPC tới AI worker,
kết quả nhận về qua HTTP/2 callback.

### Kiến trúc

```
[test-pcm-real-ai.sh]
      │  POST /v1/sessions  (+ callback_url)
      │  UDP 3000 RTP packets  (speech.pcmu, 60s PCMU 8kHz)
      ▼
[media-ai-gateway :8080]
      │  decode PCMU → PCM 16kHz
      │  chunk 500ms  →  120 AudioChunks
      │  gRPC protobuf  →  AI worker :50051
      │
      │  kết quả từ AI → dispatcher
      │  HTTP/2 POST  →  mock-callback-server
      ▼
[mock-callback-server :9999]
      └─  log mỗi kết quả ra stdout, thoát sau EXPECT_FINAL final
```

### Cấu trúc thư mục trên lab

```
<base>/
├── scripts/
│   └── test-pcm-real-ai.sh    ← chạy từ đây
├── data/
│   └── generated/
│       └── g711/
│           └── speech.pcmu    ← tự động tìm (sibling của scripts/)
├── bin/
│   ├── media-ai-gateway       ← build trước
│   ├── mock-rtp-sender        ← build trước
│   └── mock-callback-server   ← build trước
/etc/gateway/
└── gateway-mock.yaml          ← tự động tìm
```

Script tự resolve tất cả paths từ vị trí của chính nó — chạy từ bất kỳ thư mục nào đều đúng.

### Bước 1 — Build binaries (1 lần)

Từ thư mục source của project:

```bash
go build -o <base>/bin/media-ai-gateway     ./cmd/media-ai-gateway/
go build -o <base>/bin/mock-rtp-sender      ./cmd/mock-rtp-sender/
go build -o <base>/bin/mock-callback-server ./cmd/mock-callback-server/
```

### Bước 2 — Chạy với mock-ai-worker (không cần AI thật)

**Terminal 1 — mock-ai-worker:**

```bash
<base>/bin/mock-ai-worker --addr :50051 --log-level info
```

**Terminal 2 — test:**

```bash
bash <base>/scripts/test-pcm-real-ai.sh
```

Script tự tìm:
- Config: `/etc/gateway/gateway-mock.yaml`
- PCM file: `<base>/data/generated/g711/speech.pcmu`
- Binaries: `<base>/bin/`

Pipeline math (mock-ai-worker):

```
3000 RTP frames  →  120 AudioChunks (500ms/chunk)
120 chunks  →  partial mỗi 3  =  40 partial
             →  final   mỗi 6  =  20 final
             →  60 kết quả tổng
```

Để nhận đủ 20 final (không bị dispatcher_errors):

```bash
EXPECT_FINAL=20 bash <base>/scripts/test-pcm-real-ai.sh
```

### Bước 3 — Chạy với AI thật

```bash
# AI worker cùng máy
bash <base>/scripts/test-pcm-real-ai.sh

# AI worker trên máy khác
AI_ADDR=192.168.1.10:50051 EXPECT_FINAL=5 bash <base>/scripts/test-pcm-real-ai.sh

# Tăng timeout nếu AI xử lý chậm
AI_ADDR=192.168.1.10:50051 EXPECT_FINAL=5 CALLBACK_TIMEOUT=300 bash <base>/scripts/test-pcm-real-ai.sh
```

### Biến môi trường

| Biến | Mặc định | Ý nghĩa |
|---|---|---|
| `AI_ADDR` | `127.0.0.1:50051` | AI worker gRPC address |
| `GW_PORT` | `8080` | Gateway HTTP port |
| `CALLBACK_PORT` | `9999` | mock-callback-server listen port |
| `EXPECT_FINAL` | `1` | Số final callback tối thiểu để PASS |
| `CALLBACK_TIMEOUT` | `120` | Giây chờ tổng cộng cho callback |
| `PCM_FILE` | auto (`<base>/data/generated/g711/speech.pcmu`) | Override path file PCMU |
| `BASE_CONFIG` | auto (`/etc/gateway/gateway-mock.yaml` → `<base>/config/gateway-mock.yaml`) | Override config YAML |
| `GW_BIN` | auto (`<base>/bin/media-ai-gateway`) | Override gateway binary path |
| `SENDER_BIN` | auto (`<base>/bin/mock-rtp-sender`) | Override sender binary path |
| `CALLBACK_BIN` | auto (`<base>/bin/mock-callback-server`) | Override callback binary path |

### Kết quả kỳ vọng (mock-ai-worker, EXPECT_FINAL=20)

```
╔══════════════════════════════════════════════════════════╗
║  PCM Real AI Test  (speech.pcmu → AI thật → callback)   ║
╚══════════════════════════════════════════════════════════╝
AI_ADDR         = 127.0.0.1:50051
PCM_FILE        = <base>/data/generated/g711/speech.pcmu
EXPECT_FINAL    = 20

Step 1 — Preflight
[OK]    speech.pcmu  480044 bytes = 3000 frames = 60s ✓
[OK]    /etc/gateway/gateway-mock.yaml ✓

Step 2 — Tạo gateway config với ai.grpc_target = 127.0.0.1:50051

Step 3 — Khởi động gateway
[OK]    Gateway ready ✓

Step 7 — Gửi 3000 PCMU frames (60s) → 127.0.0.1:40xxx
[OK]    RTP sender hoàn thành ✓

Step 9 — Kết quả callbacks
{"event":"summary","stats":{"asr.transcript.final":20,"asr.transcript.partial":40}}

Step 10 — Metrics delta
  pool_processed  : +3000
  dispatcher_sent : +60
  ai_recv_errors  : +0

Step 11 — Kiểm tra kết quả
[OK]    final callbacks    : 20 ✓ (≥ 20)
[OK]    ai_recv_errors     : 0 ✓
[OK]    dispatcher_errors  : 0 ✓

✓ PASS — 20 final callback(s) nhận được từ AI thật
```

> **Lưu ý `dispatcher_errors`**: mock-callback-server thoát sau khi nhận đủ `EXPECT_FINAL`.
> Các kết quả còn lại → "connection refused" → `dispatcher_errors > 0`.
> Đây là hành vi bình thường — không phải lỗi pipeline.
> Dùng `EXPECT_FINAL=20` để nhận hết và `dispatcher_errors = 0`.

### Timeout behavior khi kết nối AI worker

Gateway tự động xử lý 3 loại timeout — không cần cấu hình thêm (hardcoded trong `DefaultConfig`):

| Timeout | Giá trị | Khi nào trigger |
|---|---|---|
| `FirstChunkTimeout` | **3s** | Sau khi stream mở mà pipeline chưa gửi AudioChunk đầu tiên trong 3s |
| `SendTimeout` | **500ms** | Mỗi lần `Send()` AudioChunk tới AI tốn > 500ms |
| `RecvIdleTimeout` | **30s** | AI worker không gửi kết quả nào trong 30s liên tiếp |

Thêm vào đó:
- **ErrUnexpectedEOF**: AI worker đóng stream sớm (trước khi gateway gửi `end_of_stream`) → reconnect.
- **Reconnect**: `max_retry: 2` (config `gateway-mock.yaml`), backoff 500ms → 1s.

Xem log gateway để debug timeout:

```bash
# Tất cả sự kiện AI stream
grep -E '"session_id"|"first chunk|"recv idle|"unexpected EOF|"reconnect' /tmp/gateway-real-ai.log

# Chỉ lỗi
grep '"level":"ERROR"' /tmp/gateway-real-ai.log

# Debug mode (dừng gateway, sửa config, khởi động lại)
# gateway-mock.yaml:  log.level: "debug"
```

---

## Bước 10 — Self-contained E2E Callback Test (test-callback-e2e.sh)

Script hoàn toàn tự quản lý: **tự build** tất cả binary → khởi động services → gửi RTP → verify callback → cleanup.
Không cần gateway hay mock-ai-worker đang chạy trước.

### Kiến trúc

```
[test-callback-e2e.sh]
      │  build  →  media-ai-gateway-cb, mock-ai-worker, mock-rtp-sender, mock-callback-server
      │
      │  start  →  mock-ai-worker     :50051
      │  start  →  media-ai-gateway   :8080  (config/gateway-mock.yaml)
      │  start  →  mock-callback-server :9999
      │
      │  POST /v1/sessions  (callback_url=http://127.0.0.1:9999)
      │  UDP 200 PCMU packets  →  :40xxx
      ▼
[mock-ai-worker]  →  gRPC RecognitionResult  →  [gateway dispatcher]
                                                        │  HTTP/2 POST
                                                        ▼
                                              [mock-callback-server :9999]
                                                        └─ verify ≥1 final
```

### Chạy

```bash
cd ~/media-ai/src    # hoặc thư mục chứa source
chmod +x scripts/test-callback-e2e.sh
bash scripts/test-callback-e2e.sh
```

**Không cần khởi động bất kỳ service nào trước** — script tự build và quản lý tất cả.

### Kết quả kỳ vọng

```
[INFO]  Build: media-ai-gateway-cb ✓
[INFO]  Build: mock-ai-worker ✓
[INFO]  Build: mock-rtp-sender ✓
[INFO]  Build: mock-callback-server ✓
[INFO]  mock-ai-worker started (pid=...)
[INFO]  gateway started (pid=...)
[OK]    Gateway ready ✓
[INFO]  Tạo session với callback_url=http://127.0.0.1:9999
[OK]    Session created (HTTP 201, HTTP/2.0) ✓
[OK]    RTP endpoint: 127.0.0.1:40xxx
[INFO]  Gửi 200 PCMU packets...
[OK]    mock-callback-server nhận ≥1 final ✓
[OK]    Gateway metrics: dispatcher_sent > 0 ✓
[OK]    AI errors: 0 ✓
[OK]    Callback E2E PASSED ✓
```

> **So sánh với Bước 6 (`test-rtp-callback.sh`)**: Bước 6 yêu cầu gateway và mock-ai-worker đang chạy trước; `test-callback-e2e.sh` tự quản lý toàn bộ vòng đời — phù hợp hơn cho CI hoặc chạy lần đầu trên máy mới.

---

## Bước 11 — Real AI G.711 E2E Test (test-real-ai-g711.sh)

Full end-to-end: file G.711 PCMU thật (speech.pcmu) → gateway → **real AI worker** → HTTP/2 callback.
Dùng khi cần verify kết nối với AI thật (không phải mock).

### Luồng

```
[test-real-ai-g711.sh]
      │  Step 1  Health check gateway
      │  Step 2  POST /v1/sessions  (PCMU session)
      │  Step 3  PATCH /v1/sessions/{id}  (+ callback_url + H.248 mediaResources)
      │  Step 4  Gửi data/generated/g711/speech.pcmu  →  rtp_port
      │  Step 5  Chờ real AI trả kết quả  →  gateway callback
      │  Step 6  Verify callback log + metrics
      │  Step 7  DELETE session
      ▼
[media-ai-gateway :8080]  ──gRPC──►  [Real AI worker]
      │  HTTP/2 POST callback
      ▼
[mock-callback-server :9999]
```

### Yêu cầu

- `media-ai-gateway` đang chạy và **kết nối được tới real AI** (`ai.grpc_target` trong config trỏ đúng)
- `data/generated/g711/speech.pcmu` tồn tại (file PCMU G.711, 60s = 3000 frames)
- `mock-callback-server`, `mock-rtp-sender` đã build (xem Bước 1)

### Chạy

```bash
cd ~/media-ai
chmod +x scripts/test-real-ai-g711.sh

# Mặc định: 250 packets (~5s), chờ 1 final, timeout 60s
bash scripts/test-real-ai-g711.sh

# Gửi toàn bộ file (~58s audio), chờ nhiều final hơn
RTP_PACKETS=0 EXPECT_FINAL=3 CALLBACK_TIMEOUT=120 bash scripts/test-real-ai-g711.sh

# AI worker trên máy khác
AI_ADDR=192.168.1.10:50051 bash scripts/test-real-ai-g711.sh 127.0.0.1 8080
```

### Biến môi trường

| Biến | Mặc định | Ý nghĩa |
|---|---|---|
| `CALLBACK_PORT` | `9999` | mock-callback-server listen port |
| `CALLBACK_HOST` | `127.0.0.1` | Host mà gateway gọi callback về |
| `RTP_PACKETS` | `250` | Số packet gửi (0 = toàn bộ file, ~3000 frames = 60s) |
| `EXPECT_FINAL` | `1` | Số final callback tối thiểu để PASS |
| `CALLBACK_TIMEOUT` | `60` | Giây chờ tổng cộng cho callback |
| `LANGUAGE` | `vi` | Ngôn ngữ gửi AI trong session create |
| `TASK` | `transcribe` | Task gửi AI trong session create |

### Kết quả kỳ vọng

```
[INFO]  Preflight: speech.pcmu 480044 bytes ✓
[OK]    Gateway ready ✓
[OK]    Session created (HTTP 201) ✓
[OK]    Session patched: callback_url + mediaResources ✓
[OK]    RTP sender hoàn thành (250 packets) ✓
[OK]    Nhận ≥1 final callback ✓
[OK]    ai_recv_errors: 0 ✓
[OK]    dispatcher_errors: 0 ✓
[OK]    Real AI G.711 E2E PASSED ✓
```

### Lưu ý về PATCH session (H.248 mediaResources)

Script tự động gửi PATCH sau khi tạo session để gán `callback_url` và IMS identity:

```bash
curl -s --http2-prior-knowledge \
  -X PATCH http://gateway:8080/v1/sessions/$SESSION_ID \
  -H "Content-Type: application/json" \
  -d '{
    "callback_url": "http://127.0.0.1:9999",
    "mediaResources": {
      "tAccess": {
        "contextId": "ctx-g711-test",
        "terminationId": "term-g711-test",
        "endpoint": "127.0.0.1:5004"
      }
    }
  }'
```

Các callback sau PATCH sẽ có thêm `contextId` và `terminationId` — xem `docs/callback.md` để biết chi tiết.

---

## Troubleshooting

| Triệu chứng | Nguyên nhân | Cách fix |
|---|---|---|
| `AI gRPC stream active = 0` | mock-ai-worker chưa chạy | Khởi động terminal 1 trước |
| Không thấy `"ai: gRPC connection initiated"` khi start | `grpc_target` bị trống | Kiểm tra `ai.grpc_target: "127.0.0.1:50051"` trong config |
| `AI send errors > 0` | gRPC send timeout hoặc connection mất | Tăng `send_timeout_ms: 2000`; kiểm tra mock-ai-worker đang chạy |
| `Session đã biến mất` | Idle GC (30s) | Tăng `idle_timeout_sec: 120` |
| `Audio jobs submitted = 0` | RTP không đến port | Kiểm tra firewall UDP 40000–40099 |
| `Audio jobs processed = 0` | Decode error | Xác nhận `codec: PCMU`, `sample_rate: 8000` |
| Mock không log `stream opened` | gRPC không kết nối | Kiểm tra `grpc_target: "127.0.0.1:50051"` |
| `HTTP 503 no_ai_worker` | gateway chưa thấy mock | Chờ 2–3s sau khi start gateway rồi test |
| `Callback E2E: 0 final callback` | mock-ai-worker chưa gửi final | Kiểm tra đủ 200 packets (~8 AudioChunk) |
| `result_sent_total không tăng` | callback_url không được set | Đảm bảo tạo session có `callback_url` |
| `mock-callback-server: address in use` | Port 9999 bị chiếm | Đặt `CALLBACK_PORT=9998` hoặc kill process cũ |
| `mock-callback-server: build failed` | Go không trong PATH | Build thủ công: `go build ./cmd/mock-callback-server` |
| `callback: preconnect failed` khi start gateway | mock-callback-server chưa chạy | Bình thường — start mock-callback-server trước để có pre-connect; gateway vẫn hoạt động không cần pre-connect |

**Lỗi liên quan test-pcm-real-ai.sh:**

| Triệu chứng | Nguyên nhân | Cách fix |
|---|---|---|
| `final callbacks: 0` | AI worker chưa khởi động | Start AI worker tại `AI_ADDR` trước khi test |
| `ai_recv_errors > 0`, log: `first chunk timeout` | Pipeline khởi động > 3s | Kiểm tra jitter buffer, `chunk_ms` trong config |
| `ai_recv_errors > 0`, log: `recv idle timeout` | AI worker treo, không trả kết quả trong 30s | Xem log AI worker, kiểm tra GPU/CPU |
| `ai_recv_errors > 0`, log: `unexpected EOF` | AI worker đóng stream trước khi nhận `end_of_stream` | AI worker có idle timeout < khoảng cách giữa các chunk — cần cấu hình AI tăng timeout |
| `dispatcher_errors > 0` nhưng `ai_recv_errors = 0` | mock-callback-server đã thoát trước khi nhận hết | Bình thường — tăng `EXPECT_FINAL` bằng số final thực tế |
| `CALLBACK_TIMEOUT` hết, không đủ final | AI worker xử lý chậm | Tăng `CALLBACK_TIMEOUT=300` |
| `pool_processed < 3000` | Mất packet UDP | Chạy test trên loopback (127.0.0.1), kiểm tra firewall |

### Xem log chi tiết

```bash
# Gateway log realtime
tail -f /tmp/gw.log | python3 -m json.tool

# Hoặc filter lỗi
grep '"level":"ERROR"' /tmp/gw.log

# Đổi sang debug mode (restart gateway)
# Sửa gateway-mock.yaml:  log.level: "debug"
```

### Kiểm tra trạng thái kết nối (Connection Status API)

```bash
# Xem toàn bộ: AI gRPC state, callback H/2, RTP ports đang mở
curl -s --http2-prior-knowledge http://127.0.0.1:8080/v1/connections | python3 -m json.tool
```

Kết quả kỳ vọng khi cả 3 service đang chạy:

```json
{
  "ai_workers": [{ "addr": "127.0.0.1:50051", "state": "READY" }],
  "callback":   { "url": "http://127.0.0.1:9999/", "connected": true, "preconnect_at": "..." },
  "rtp":        { "per_session_open": 0, "per_session_capacity": 100, "shared_ingress": true }
}
```

- `ai_workers[0].state = "CONNECTING"` → mock-ai-worker chưa accept TCP, chờ thêm 2-3s
- `ai_workers[0].state = "TRANSIENT_FAILURE"` → mock-ai-worker không chạy hoặc sai port
- `callback.connected = false` → mock-callback-server chưa chạy khi gateway start (không sao, callback vẫn hoạt động)
- `rtp.per_session_open = N` → có N session đang giữ per-session RTP port

### Kiểm tra port thủ công

```bash
# Xác nhận gateway đang lắng nghe
ss -tlnp | grep 8080
ss -tlnp | grep 50051

# Xác nhận per-session UDP port sau khi tạo session
ss -ulnp | grep 40000

# Gọi health check
curl -s http://127.0.0.1:8080/health/ready
```

---

## Chạy lại test (cleanup)

```bash
# Dừng tất cả
pkill media-ai-gateway
pkill mock-ai-worker

# Xoá log cũ
rm -f /tmp/gw.log

# Chạy lại từ Bước 4
```
