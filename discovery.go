package zap

import (
	"time"

	"go.uber.org/zap"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func Discovery(log *zap.Logger, details trace.Details) (t trace.Discovery) {
	if details&trace.DiscoveryEvents != 0 {
		log = log.Named("discovery")
		t.OnDiscover = func(info trace.DiscoverStartInfo) func(trace.DiscoverDoneInfo) {
			log.Info("try to discover",
				zap.String("version", version),
			)
			start := time.Now()
			return func(info trace.DiscoverDoneInfo) {
				if info.Error == nil {
					endpoints := make([]string, 0, len(info.Endpoints))
					for _, e := range info.Endpoints {
						endpoints = append(endpoints, e.String())
					}
					log.Info("discover finished",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.Strings("endpoints", endpoints),
					)
				} else {
					log.Error("discover failed",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.Error(info.Error),
					)
				}
			}
		}
	}
	return t
}
