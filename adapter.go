package zap

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	"go.uber.org/zap"
)

var _ log.Logger = adapter{}

type adapter struct {
	l *zap.Logger
}

func (a adapter) Log(params log.Params, msg string, fields ...log.Field) {
	l := a.l
	for _, name := range params.Namespace {
		l = l.Named(name)
	}
	switch params.Level {
	case log.TRACE, log.DEBUG:
		l.Debug(msg, fieldsToFields(fields)...)
	case log.INFO:
		l.Info(msg, fieldsToFields(fields)...)
	case log.WARN:
		l.Warn(msg, fieldsToFields(fields)...)
	case log.ERROR:
		l.Error(msg, fieldsToFields(fields)...)
	case log.FATAL:
		l.Fatal(msg, fieldsToFields(fields)...)
	default:
	}
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

func fieldsToFields(fields []log.Field) []zap.Field {
	ff := make([]zap.Field, len(fields))
	for i, f := range fields {
		ff[i] = fieldToField(f)
	}
	return ff
}
