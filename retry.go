package zap

import (
	"time"

	"go.uber.org/zap"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	ydbRetry "github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func Retry(log *zap.Logger, d detailer) (t trace.Retry) {
	if d.Details()&trace.RetryEvents != 0 {
		retry := log.Named("retry")
		t.OnRetry = func(info trace.RetryLoopStartInfo) func(trace.RetryLoopIntermediateInfo) func(trace.RetryLoopDoneInfo) {
			idempotent := info.Idempotent
			if info.NestedCall {
				retry.Error("nested call")
			}
			retry.Debug("init",
				zap.Bool("idempotent", idempotent),
			)
			start := time.Now()
			return func(info trace.RetryLoopIntermediateInfo) func(doneInfo trace.RetryLoopDoneInfo) {
				if info.Error == nil {
					retry.Debug("attempt",
						zap.Duration("latency", time.Since(start)),
						zap.Bool("idempotent", idempotent),
					)
				} else {
					f := retry.Warn
					if !ydb.IsYdbError(info.Error) {
						f = retry.Debug
					}
					m := ydbRetry.Check(info.Error)
					f("intermediate",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.Bool("idempotent", idempotent),
						zap.Bool("retryable", m.MustRetry(idempotent)),
						zap.Bool("deleteSession", m.MustDeleteSession()),
						zap.Int64("code", m.StatusCode()),
						zap.Error(info.Error),
					)
				}
				return func(info trace.RetryLoopDoneInfo) {
					if info.Error == nil {
						retry.Debug("finish",
							zap.Duration("latency", time.Since(start)),
							zap.Bool("idempotent", idempotent),
							zap.Int("attempts", info.Attempts),
						)
					} else {
						f := retry.Error
						if !ydb.IsYdbError(info.Error) {
							f = retry.Debug
						}
						m := ydbRetry.Check(info.Error)
						f("done",
							zap.String("version", version),
							zap.Duration("latency", time.Since(start)),
							zap.Bool("idempotent", idempotent),
							zap.Bool("retryable", m.MustRetry(idempotent)),
							zap.Bool("deleteSession", m.MustDeleteSession()),
							zap.Int64("code", m.StatusCode()),
							zap.Error(info.Error),
						)
					}
				}
			}
		}
	}
	return t
}
