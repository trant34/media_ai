# Hướng dẫn thiết lập test lab với mock-ai-worker

## Tổng quan

### Test cơ bản (lab-rtp-test.sh)

```
[lab-rtp-test.sh]
      │  HTTP/2  POST /v1/sessions
      ▼
[media-ai-gateway :8080]
      │  per-session UDP  :40000–40099
      │  gRPC JSON  →  mock-ai-worker :50051
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
scp bin/media-ai-gateway        $LAB:~/media-ai/
scp bin/mock-ai-worker          $LAB:~/media-ai/
scp bin/mock-callback-server    $LAB:~/media-ai/
scp config/gateway-mock.yaml    $LAB:~/media-ai/
scp scripts/lab-rtp-test.sh     $LAB:~/media-ai/
scp scripts/test-rtp-callback.sh $LAB:~/media-ai/
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
{"msg":"AI routing dialer active","target":"127.0.0.1:50051"}
{"msg":"HTTP/2 control plane listening","addr":":8080","mode":"h2c"}
{"msg":"RTP ingress listening","addr":":5004"}
```

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

---

## Troubleshooting

| Triệu chứng | Nguyên nhân | Cách fix |
|---|---|---|
| `AI gRPC stream active = 0` | mock-ai-worker chưa chạy | Khởi động terminal 1 trước |
| `AI send errors > 0` | gRPC send timeout | Tăng `send_timeout_ms: 2000` trong config |
| `Session đã biến mất` | Idle GC (30s) | Tăng `idle_timeout_sec: 120` |
| `Audio jobs submitted = 0` | RTP không đến port | Kiểm tra firewall UDP 40000–40099 |
| `Audio jobs processed = 0` | Decode error | Xác nhận `codec: PCMU`, `sample_rate: 8000` |
| Mock không log `stream opened` | gRPC không kết nối | Kiểm tra `grpc_target: "127.0.0.1:50051"` |
| `HTTP 503 no_ai_worker` | gateway chưa thấy mock | Chờ 2–3s sau khi start gateway rồi test |
| `Callback E2E: 0 final callback` | mock-ai-worker chưa gửi final | Kiểm tra đủ 200 packets (~8 AudioChunk) |
| `result_sent_total không tăng` | callback_url không được set | Đảm bảo tạo session có `callback_url` |
| `mock-callback-server: address in use` | Port 9999 bị chiếm | Đặt `CALLBACK_PORT=9998` hoặc kill process cũ |
| `mock-callback-server: build failed` | Go không trong PATH | Build thủ công: `go build ./cmd/mock-callback-server` |

### Xem log chi tiết

```bash
# Gateway log realtime
tail -f /tmp/gw.log | python3 -m json.tool

# Hoặc filter lỗi
grep '"level":"ERROR"' /tmp/gw.log

# Đổi sang debug mode (restart gateway)
# Sửa gateway-mock.yaml:  log.level: "debug"
```

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
