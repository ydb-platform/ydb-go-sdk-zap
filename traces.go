package zap

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"go.uber.org/zap"

	"github.com/ydb-platform/ydb-go-sdk/v3"
)

type Option = log.Option

func WithLogQuery() Option {
	return log.WithLogQuery()
}

func WithTraces(l *zap.Logger, d trace.Detailer, opts ...Option) ydb.Option {
	a := adapter{l: l}
	return ydb.MergeOptions(
		ydb.WithTraceDriver(log.Driver(a, d, opts...)),
		ydb.WithTraceTable(log.Table(a, d, opts...)),
		ydb.WithTraceScripting(log.Scripting(a, d, opts...)),
		ydb.WithTraceScheme(log.Scheme(a, d, opts...)),
		ydb.WithTraceCoordination(log.Coordination(a, d, opts...)),
		ydb.WithTraceRatelimiter(log.Ratelimiter(a, d, opts...)),
		ydb.WithTraceDiscovery(log.Discovery(a, d, opts...)),
		ydb.WithTraceTopic(log.Topic(a, d, opts...)),
		ydb.WithTraceDatabaseSQL(log.DatabaseSQL(a, d, opts...)),
	)
}

func WithLogger(l *zap.Logger, d trace.Detailer, opts ...Option) ydb.Option {
	return ydb.WithLogger(adapter{l: l}, d, opts...)
}

func Table(l *zap.Logger, d trace.Detailer, opts ...log.Option) trace.Table {
	return log.Table(&adapter{l: l}, d, opts...)
}

func Topic(l *zap.Logger, d trace.Detailer, opts ...log.Option) trace.Topic {
	return log.Topic(&adapter{l: l}, d, opts...)
}

func Driver(l *zap.Logger, d trace.Detailer, opts ...log.Option) trace.Driver {
	return log.Driver(&adapter{l: l}, d, opts...)
}

func Coordination(l *zap.Logger, d trace.Detailer, opts ...log.Option) trace.Coordination {
	return log.Coordination(&adapter{l: l}, d, opts...)
}

func Discovery(l *zap.Logger, d trace.Detailer, opts ...log.Option) trace.Discovery {
	return log.Discovery(&adapter{l: l}, d, opts...)
}

func Ratelimiter(l *zap.Logger, d trace.Detailer, opts ...log.Option) trace.Ratelimiter {
	return log.Ratelimiter(&adapter{l: l}, d, opts...)
}

func Scheme(l *zap.Logger, d trace.Detailer, opts ...log.Option) trace.Scheme {
	return log.Scheme(&adapter{l: l}, d, opts...)
}

func Scripting(l *zap.Logger, d trace.Detailer, opts ...log.Option) trace.Scripting {
	return log.Scripting(&adapter{l: l}, d, opts...)
}

func DatabaseSQL(l *zap.Logger, d trace.Detailer, opts ...log.Option) trace.DatabaseSQL {
	return log.DatabaseSQL(&adapter{l: l}, d, opts...)
}
