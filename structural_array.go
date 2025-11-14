package zap

import (
	"fmt"
	"math"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/log/structural"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type array struct {
	items []zap.Field
}

func appendTo(f zap.Field, enc zapcore.ArrayEncoder) error {
	// this function is a modified version of zapcore.Field.AddTo

	var err error

	switch f.Type {
	case zapcore.ArrayMarshalerType:
		err = enc.AppendArray(f.Interface.(zapcore.ArrayMarshaler))
	case zapcore.ObjectMarshalerType:
		err = enc.AppendObject(f.Interface.(zapcore.ObjectMarshaler))
	case zapcore.InlineMarshalerType:
		break
	case zapcore.BinaryType:
		// seems that ArrayEncoder does not suppoer non-UTF-8 binary blobs
		// enc.AppendBinary(f.Interface.([]byte))
		break
	case zapcore.BoolType:
		enc.AppendBool(f.Integer == 1)
	case zapcore.ByteStringType:
		enc.AppendByteString(f.Interface.([]byte))
	case zapcore.Complex128Type:
		enc.AppendComplex128(f.Interface.(complex128))
	case zapcore.Complex64Type:
		enc.AppendComplex64(f.Interface.(complex64))
	case zapcore.DurationType:
		enc.AppendDuration(time.Duration(f.Integer))
	case zapcore.Float64Type:
		enc.AppendFloat64(math.Float64frombits(uint64(f.Integer)))
	case zapcore.Float32Type:
		enc.AppendFloat32(math.Float32frombits(uint32(f.Integer)))
	case zapcore.Int64Type:
		enc.AppendInt64(f.Integer)
	case zapcore.Int32Type:
		enc.AppendInt32(int32(f.Integer))
	case zapcore.Int16Type:
		enc.AppendInt16(int16(f.Integer))
	case zapcore.Int8Type:
		enc.AppendInt8(int8(f.Integer))
	case zapcore.StringType:
		enc.AppendString(f.String)
	case zapcore.TimeType:
		if f.Interface != nil {
			enc.AppendTime(time.Unix(0, f.Integer).In(f.Interface.(*time.Location)))
		} else {
			// Fall back to UTC if location is nil.
			enc.AppendTime(time.Unix(0, f.Integer))
		}
	case zapcore.TimeFullType:
		enc.AppendTime(f.Interface.(time.Time))
	case zapcore.Uint64Type:
		enc.AppendUint64(uint64(f.Integer))
	case zapcore.Uint32Type:
		enc.AppendUint32(uint32(f.Integer))
	case zapcore.Uint16Type:
		enc.AppendUint16(uint16(f.Integer))
	case zapcore.Uint8Type:
		enc.AppendUint8(uint8(f.Integer))
	case zapcore.UintptrType:
		enc.AppendUintptr(uintptr(f.Integer))
	case zapcore.ReflectType:
		err = enc.AppendReflected(f.Interface)
	case zapcore.NamespaceType:
		break
	case zapcore.StringerType:
		// zapcore.Field.AddTo() recovers from panic here
		enc.AppendString(f.Interface.(fmt.Stringer).String())
	case zapcore.ErrorType:
		// zapcore.Field.AddTo() recovers from panic and provides rich error formatting here
		enc.AppendString(f.Interface.(error).Error())
	case zapcore.SkipType:
		break
	default:
		panic(fmt.Sprintf("ydp-zap: unknown field type: %v", f))
	}

	return err
}

func (a *array) MarshalLogArray(enc zapcore.ArrayEncoder) error {
	for _, item := range a.items {
		if err := appendTo(item, enc); err != nil {
			return err
		}
	}
	return nil
}

func (a *array) Object(value structural.Record) structural.Array {
	rec, ok := value.(*record)
	if !ok {
		panic("ydb-zap: unsupported Record")
	}
	a.items = append(a.items, zap.Object("", rec))
	return a
}

func (a *array) Array(value structural.Array) structural.Array {
	arr, ok := value.(*array)
	if !ok {
		panic("ydb-zap: unsupported Array")
	}
	a.items = append(a.items, zap.Array("", arr))
	return a
}

func (a *array) String(value string) structural.Array {
	a.items = append(a.items, zap.String("", value))
    return a
}

func (a *array) Strings(value []string) structural.Array {
	a.items = append(a.items, zap.Strings("", value))
    return a
}

func (a *array) Stringer(value fmt.Stringer) structural.Array {
	a.items = append(a.items, zap.Stringer("", value))
    return a
}

func (a *array) Duration(value time.Duration) structural.Array {
	a.items = append(a.items, zap.Duration("", value))
    return a
}

func (a *array) Int(value int) structural.Array {
	a.items = append(a.items, zap.Int("", value))
    return a
}

func (a *array) Int8(value int8) structural.Array {
	a.items = append(a.items, zap.Int8("", value))
    return a
}

func (a *array) Int16(value int16) structural.Array {
	a.items = append(a.items, zap.Int16("", value))
    return a
}

func (a *array) Int32(value int32) structural.Array {
	a.items = append(a.items, zap.Int32("", value))
    return a
}

func (a *array) Int64(value int64) structural.Array {
	a.items = append(a.items, zap.Int64("", value))
    return a
}

func (a *array) Uint(value uint) structural.Array {
	a.items = append(a.items, zap.Uint("", value))
    return a
}

func (a *array) Uint8(value uint8) structural.Array {
	a.items = append(a.items, zap.Uint8("", value))
    return a
}

func (a *array) Uint16(value uint16) structural.Array {
	a.items = append(a.items, zap.Uint16("", value))
    return a
}

func (a *array) Uint32(value uint32) structural.Array {
	a.items = append(a.items, zap.Uint32("", value))
    return a
}

func (a *array) Uint64(value uint64) structural.Array {
	a.items = append(a.items, zap.Uint64("", value))
    return a
}

func (a *array) Float32(value float32) structural.Array {
	a.items = append(a.items, zap.Float32("", value))
    return a
}

func (a *array) Float64(value float64) structural.Array {
	a.items = append(a.items, zap.Float64("", value))
    return a
}

func (a *array) Bool(value bool) structural.Array {
	a.items = append(a.items, zap.Bool("", value))
    return a
}

func (a *array) Error(value error) structural.Array {
	a.items = append(a.items, zap.Error(value))
    return a
}

func (a *array) NamedError(value error) structural.Array {
	a.items = append(a.items, zap.NamedError("", value))
    return a
}

func (a *array) Any(value interface{}) structural.Array {
	a.items = append(a.items, zap.Any("", value))
    return a
}