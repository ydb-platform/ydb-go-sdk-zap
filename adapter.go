package zap

import (
	"context"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var _ log.Logger = adapter{}

type adapter struct {
	l *zap.Logger
}

func (a adapter) Log(ctx context.Context, msg string, fields ...log.Field) {
	l := a.l
	for _, name := range log.NamesFromContext(ctx) {
		l = l.Named(name)
	}
	l.Log(Level(ctx), msg, Fields(fields)...)
}

func fieldToField(field log.Field) zap.Field {
	switch field.Type() {
	case log.IntType:
		return zap.Int(field.Key(), field.IntValue())
	case log.Int64Type:
		return zap.Int64(field.Key(), field.Int64Value())
	case log.StringType:
		return zap.String(field.Key(), field.StringValue())
	case log.BoolType:
		return zap.Bool(field.Key(), field.BoolValue())
	case log.DurationType:
		return zap.Duration(field.Key(), field.DurationValue())
	case log.StringsType:
		return zap.Strings(field.Key(), field.StringsValue())
	case log.ErrorType:
		return zap.Error(field.ErrorValue())
	case log.StringerType:
		return zap.Stringer(field.Key(), field.Stringer())
	default:
		return zap.Any(field.Key(), field.AnyValue())
	}
}

func Fields(fields []log.Field) []zap.Field {
	ff := make([]zap.Field, len(fields))
	for i, f := range fields {
		ff[i] = fieldToField(f)
	}
	return ff
}

func Level(ctx context.Context) zapcore.Level {
	switch log.LevelFromContext(ctx) {
	case log.TRACE, log.DEBUG:
		return zapcore.DebugLevel
	case log.INFO:
		return zapcore.InfoLevel
	case log.WARN:
		return zapcore.WarnLevel
	case log.ERROR:
		return zapcore.ErrorLevel
	case log.FATAL:
		return zapcore.FatalLevel
	default:
		return zapcore.InvalidLevel
	}
}
