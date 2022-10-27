package zap

import (
	"time"

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

func (l *logger) Record() structural.Record {
	return &record{
		l: l,
	}
}

func (l *logger) WithName(name string) structural.Logger {
	return Logger(l.l.Named(name))
}

func (l *logger) WithCallerSkip(n int) structural.Logger {
	return Logger(l.l.WithOptions(zap.AddCallerSkip(n)))
}

type record struct {
	l *logger
	level zapcore.Level
	fields []zap.Field
}

func (r *record) Level(lvl structural.Level) structural.Record {
	switch lvl {
	case structural.TRACE:
		r.level = zapcore.DebugLevel
	case structural.DEBUG:
		r.level = zapcore.DebugLevel
	case structural.INFO:
		r.level = zapcore.InfoLevel
	case structural.WARN:
		r.level = zapcore.WarnLevel
	case structural.ERROR:
		r.level = zapcore.ErrorLevel
	case structural.FATAL:
		r.level = zapcore.FatalLevel
	default:
		r.level = zapcore.DebugLevel
	}
	return r
}

func (r *record) String(key string, value string) structural.Record {
	r.fields = append(r.fields, zap.String(key, value))
	return r
}

func (r *record) Strings(key string, value []string) structural.Record {
	r.fields = append(r.fields, zap.Strings(key, value))
	return r
}

func (r *record) Duration(key string, value time.Duration) structural.Record {
	r.fields = append(r.fields, zap.Duration(key, value))
	return r
}

func (r *record) Error(value error) structural.Record {
	r.fields = append(r.fields, zap.Error(value))
	return r
}

func (r *record) Reset() {
	r.level = zapcore.InfoLevel
	r.fields = r.fields[:0]
}

func (r *record) Message(msg string) {
	ce := r.l.l.Check(r.level, msg)
	if ce != nil {
		ce.Write(r.fields...)
	}
}

