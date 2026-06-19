# media-ai-gateway

Realtime audio gateway — nhận RTP/WebRTC từ hạ tầng IMS/telecom, xử lý audio, gửi sang AI worker (STT/translate) qua gRPC streaming, trả kết quả về backend qua HTTP callback hoặc WebRTC DataChannel.

## Deploy

```bash
# Build image
docker build -f deploy/Dockerfile -t registry.local/media-ai-gateway:latest .
docker push registry.local/media-ai-gateway:latest

# Apply lên Kubernetes
kubectl apply -f deploy/k8s/network-attachment.yaml
kubectl apply -f deploy/k8s/configmap.yaml
kubectl apply -f deploy/k8s/deployment.yaml
kubectl apply -f deploy/k8s/service.yaml
```

> Xem `deploy/k8s/network-attachment.yaml` để chỉnh `master` interface và subnet macvlan trước khi apply.

## Documentation

| File | Mô tả |
|------|-------|
| [docs/openapi.yaml](docs/openapi.yaml) | OpenAPI 3.0.3 spec — tất cả REST endpoints, schemas, examples |
| [docs/media-ai-gateway-design-modules_v2.md](docs/media-ai-gateway-design-modules_v2.md) | Thiết kế kiến trúc hiện hành — module breakdown, luồng dữ liệu, API, config đầy đủ |
| [docs/media-ai-gateway-design-modules_v1.md](docs/media-ai-gateway-design-modules_v1.md) | Phiên bản thiết kế ban đầu (tham khảo) |
| [docs/pion-webrtc-analysis.md](docs/pion-webrtc-analysis.md) | Phân tích pion/webrtc v4 — lựa chọn API, ICE/DTLS/SRTP, DataChannel |
| [docs/stack_webrtc.md](docs/stack_webrtc.md) | IMS Data Channel setup modes (HTTP Proxy / UDP Proxy) |
| [docs/webrct_ims_data_channel.md](docs/webrct_ims_data_channel.md) | WebRTC IMS Data Channel — ghi chú tích hợp |
| [docs/23228-j00.docx](docs/23228-j00.docx) | 3GPP TS 23.228 Annex AC — IMS Data Channel spec (tham chiếu chuẩn) |
