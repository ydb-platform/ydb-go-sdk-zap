package zap

import (
	"go.uber.org/zap"

	"github.com/ydb-platform/ydb-go-sdk/v3"
)

func WithTraces(l *zap.Logger, d detailer, opts ...option) ydb.Option {
	return ydb.MergeOptions(
		ydb.WithTraceDriver(Driver(l, d, opts...)),
		ydb.WithTraceTable(Table(l, d, opts...)),
		ydb.WithTraceScripting(Scripting(l, d, opts...)),
		ydb.WithTraceScheme(Scheme(l, d, opts...)),
		ydb.WithTraceCoordination(Coordination(l, d, opts...)),
		ydb.WithTraceRatelimiter(Ratelimiter(l, d, opts...)),
		ydb.WithTraceDiscovery(Discovery(l, d, opts...)),
		ydb.WithTraceTopic(Topic(l, d, opts...)),
		ydb.WithTraceDatabaseSQL(DatabaseSQL(l, d, opts...)),
	)
}
