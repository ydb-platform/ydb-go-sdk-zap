package zap

import (
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/log/structural"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type logger struct {
	l *zap.Logger
}

func Logger(l *zap.Logger) structural.Logger {
	return &logger{l: l.WithOptions(zap.AddCallerSkip(1))}
}

var fieldsPool = &sync.Pool{New: func () interface{} {
	return make([]zap.Field, 0)
}}

func (l *logger) record(level zapcore.Level) *record {
	return &record{
		l: l,
		level: level,
		fields: fieldsPool.Get().([]zap.Field),
	}
}

func (l *logger) Trace() structural.Record {
	return l.record(zapcore.DebugLevel)
}

func (l *logger) Debug() structural.Record {
	return l.record(zapcore.DebugLevel)
}

func (l *logger) Info() structural.Record {
	return l.record(zapcore.InfoLevel)
}

func (l *logger) Warn() structural.Record {
	return l.record(zapcore.WarnLevel)
}

func (l *logger) Error() structural.Record {
	return l.record(zapcore.ErrorLevel)
}

func (l *logger) Fatal() structural.Record {
	return l.record(zapcore.FatalLevel)
}

func (l *logger) WithName(name string) structural.Logger {
	return Logger(l.l.Named(name))
}

func (l *logger) Object() structural.Record {
	return &record{
		fields: fieldsPool.Get().([]zap.Field),
	}
}

func (l *logger) Array() structural.Array {
	return &array{
		items: fieldsPool.Get().([]zap.Field),
	}
}
