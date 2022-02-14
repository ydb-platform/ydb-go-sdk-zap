package zap

import (
	"go.uber.org/zap"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func Scheme(log *zap.Logger, details trace.Details) (t trace.Scheme) {
	return t
}
