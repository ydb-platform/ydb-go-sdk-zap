package zap

import (
	"go.uber.org/zap"
	"time"

	ydbRetry "github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func Retry(log *zap.Logger, details trace.Details) (t trace.Retry) {
	if details&trace.RetryEvents != 0 {
		retry := log.Named("retry")
		t.OnRetry = func(info trace.RetryLoopStartInfo) func(trace.RetryLoopIntermediateInfo) func(trace.RetryLoopDoneInfo) {
			idempotent := info.Idempotent
			retry.Debug("init",
				zap.String("version", version),
				zap.Bool("idempotent", idempotent))
			start := time.Now()
			return func(info trace.RetryLoopIntermediateInfo) func(doneInfo trace.RetryLoopDoneInfo) {
				if info.Error == nil {
					retry.Debug("attempt",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.Bool("idempotent", idempotent),
					)
				} else {
					log := retry.Warn
					m := ydbRetry.Check(info.Error)
					if m.StatusCode() < 0 {
						log = retry.Debug
					}
					log("intermediate",
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
							zap.String("version", version),
							zap.Duration("latency", time.Since(start)),
							zap.Bool("idempotent", idempotent),
							zap.Int("attempts", info.Attempts),
						)
					} else {
						log := retry.Error
						m := ydbRetry.Check(info.Error)
						if m.StatusCode() < 0 {
							log = retry.Debug
						}
						log("done",
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
