package ai

import (
	"context"
	"fmt"
)

// GRPCDialFunc mở một bidirectional gRPC stream tới AI worker tại workerAddr.
// Là điểm tích hợp với gRPC transport layer thực; được inject vào RoutingDialer.
type GRPCDialFunc func(ctx context.Context, workerAddr, sessionID, streamID, language, task string) (StreamClient, error)

// RoutingDialer implements ai.Dialer bằng cách chọn AI worker phù hợp từ
// WorkerRegistry rồi mở stream qua GRPCDialFunc.
//
// Routing 2 bước:
//  1. Filter: loại worker không hỗ trợ language/task hoặc đã đạt giới hạn stream.
//  2. Select: chọn worker có loadScore thấp nhất (stream ratio + GPU load).
type RoutingDialer struct {
	registry *WorkerRegistry
	dialFn   GRPCDialFunc
}

// NewRoutingDialer tạo RoutingDialer với registry và dial function cho trước.
func NewRoutingDialer(registry *WorkerRegistry, dialFn GRPCDialFunc) *RoutingDialer {
	return &RoutingDialer{registry: registry, dialFn: dialFn}
}

// Dial chọn AI worker phù hợp và mở stream tới worker đó.
// Trả về ErrNoWorkerAvailable nếu không có worker nào đáp ứng yêu cầu.
func (d *RoutingDialer) Dial(ctx context.Context, sessionID, streamID, language, task string) (StreamClient, error) {
	worker, err := d.registry.Select(language, task)
	if err != nil {
		return nil, fmt.Errorf("ai: route %s/%s: %w", language, task, err)
	}
	client, err := d.dialFn(ctx, worker.Addr, sessionID, streamID, language, task)
	if err != nil {
		return nil, fmt.Errorf("ai: dial worker %s (%s): %w", worker.ID, worker.Addr, err)
	}
	return client, nil
}
