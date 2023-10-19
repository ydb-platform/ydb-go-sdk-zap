package zap

import (
	"fmt"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/log/structural"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type record struct {
	l *logger
	level zapcore.Level
	fields []zap.Field
}

func (r *record) Object(key string, value structural.Record) structural.Record {
	rec, ok := value.(*record)
	if !ok {
		panic("ydb-zap: unsupported Record")
	}
	r.fields = append(r.fields, zap.Object(key, rec))
	return r
}

func (r *record) Array(key string, value structural.Array) structural.Record {
	arr, ok := value.(*array)
	if !ok {
		panic("ydb-zap: unsupported Array")
	}
	r.fields = append(r.fields, zap.Array(key, arr))
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

func (r *record) Stringer(key string, value fmt.Stringer) structural.Record {
	r.fields = append(r.fields, zap.Stringer(key, value))
    return r
}

func (r *record) Duration(key string, value time.Duration) structural.Record {
	r.fields = append(r.fields, zap.Duration(key, value))
    return r
}

func (r *record) Int(key string, value int) structural.Record {
	r.fields = append(r.fields, zap.Int(key, value))
    return r
}

func (r *record) Int8(key string, value int8) structural.Record {
	r.fields = append(r.fields, zap.Int8(key, value))
    return r
}

func (r *record) Int16(key string, value int16) structural.Record {
	r.fields = append(r.fields, zap.Int16(key, value))
    return r
}

func (r *record) Int32(key string, value int32) structural.Record {
	r.fields = append(r.fields, zap.Int32(key, value))
    return r
}

func (r *record) Int64(key string, value int64) structural.Record {
	r.fields = append(r.fields, zap.Int64(key, value))
    return r
}

func (r *record) Uint(key string, value uint) structural.Record {
	r.fields = append(r.fields, zap.Uint(key, value))
    return r
}

func (r *record) Uint8(key string, value uint8) structural.Record {
	r.fields = append(r.fields, zap.Uint8(key, value))
    return r
}

func (r *record) Uint16(key string, value uint16) structural.Record {
	r.fields = append(r.fields, zap.Uint16(key, value))
    return r
}

func (r *record) Uint32(key string, value uint32) structural.Record {
	r.fields = append(r.fields, zap.Uint32(key, value))
    return r
}

func (r *record) Uint64(key string, value uint64) structural.Record {
	r.fields = append(r.fields, zap.Uint64(key, value))
    return r
}

func (r *record) Float32(key string, value float32) structural.Record {
	r.fields = append(r.fields, zap.Float32(key, value))
    return r
}

func (r *record) Float64(key string, value float64) structural.Record {
	r.fields = append(r.fields, zap.Float64(key, value))
    return r
}

func (r *record) Bool(key string, value bool) structural.Record {
	r.fields = append(r.fields, zap.Bool(key, value))
    return r
}

func (r *record) Error(value error) structural.Record {
	r.fields = append(r.fields, zap.Error(value))
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
	if r.l == nil {
		return
	}
	ce := r.l.l.Check(r.level, msg)
	if ce != nil {
		ce.Write(r.fields...)
	}
	r.fields = r.fields[:0]
	r.l = nil
	fieldsPool.Put(r)
}

func (r *record) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	for _, field := range r.fields {
		field.AddTo(enc)
	}
	return nil
}