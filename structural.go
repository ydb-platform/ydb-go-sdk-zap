package zap

import (
	"sync"
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

var recordPool = &sync.Pool{New: func () interface{} {
	return &record{}
}}

func (l *logger) record(level zapcore.Level) *record {
	r := recordPool.Get().(*record)
	r.level = level
	r.l = l
	return r
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

type record struct {
	l *logger
	level zapcore.Level
	fields []zap.Field
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

func (r *record) Int(key string, value int) structural.Record {
	r.fields = append(r.fields, zap.Int(key, value))
	return r
}

func (r *record) Int64(key string, value int64) structural.Record {
	r.fields = append(r.fields, zap.Int64(key, value))
	return r
}

func (r *record) Bool(key string, value bool) structural.Record {
	r.fields = append(r.fields, zap.Bool(key, value))
	return r
}

func (r *record) NamedError(key string, value error) structural.Record {
	r.fields = append(r.fields, zap.NamedError(key, value))
	return r
}

func (r *record) Any(key string, value interface{}) structural.Record {
	r.fields = append(r.fields, zap.Any(key, value))
	return r
}

func (r *record) Message(msg string) {
	ce := r.l.l.Check(r.level, msg)
	if ce != nil {
		ce.Write(r.fields...)
	}
	r.fields = r.fields[:0]
	r.l = nil
	recordPool.Put(r)
}

