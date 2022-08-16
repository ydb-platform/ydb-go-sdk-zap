package zap

import (
	"go.uber.org/zap"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func Ratelimiter(log *zap.Logger, details trace.Details, opts ...option) (t trace.Ratelimiter) {
	return t
}
