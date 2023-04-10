package zap

import (
	"time"

	"go.uber.org/zap"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func Discovery(log *zap.Logger, d detailer, opts ...option) (t trace.Discovery) {
	log = log.Named("ydb").Named("discovery")
	t.OnDiscover = func(info trace.DiscoveryDiscoverStartInfo) func(trace.DiscoveryDiscoverDoneInfo) {
		address := info.Address
		database := info.Database
		if d.Details()&trace.DiscoveryEvents != 0 {
			log.Debug("try to discover",
				zap.String("address", address),
				zap.String("database", database),
			)
		}
		start := time.Now()
		return func(info trace.DiscoveryDiscoverDoneInfo) {
			if d.Details()&trace.DiscoveryEvents != 0 {
				if info.Error == nil {
					endpoints := make([]string, 0, len(info.Endpoints))
					for _, e := range info.Endpoints {
						endpoints = append(endpoints, e.String())
					}
					log.Info("discover finished",
						zap.String("address", address),
						zap.String("database", database),
						zap.Duration("latency", time.Since(start)),
						zap.Strings("endpoints", endpoints),
					)
				} else {
					log.Error("discover failed",
						zap.String("version", version),
						zap.String("address", address),
						zap.String("database", database),
						zap.Duration("latency", time.Since(start)),
						zap.Error(info.Error),
					)
				}
			}
		}
	}
	return t
}
