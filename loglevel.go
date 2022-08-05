package zap

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func logDebugWarn(logger *zap.Logger, err error, msg string, fields ...zap.Field) {
	logLevel(logger, err, zapcore.DebugLevel, zapcore.WarnLevel, msg, fields)
}

func logDebugInfo(logger *zap.Logger, err error, msg string, fields ...zap.Field) {
	logLevel(logger, err, zapcore.DebugLevel, zapcore.InfoLevel, msg, fields)
}

func logInfoWarn(logger *zap.Logger, err error, msg string, fields ...zap.Field) {
	logLevel(logger, err, zapcore.InfoLevel, zapcore.WarnLevel, msg, fields)
}

func logLevel(logger *zap.Logger, err error, okLevel, errLevel zapcore.Level, msg string, fields []zap.Field) {
	level := okLevel
	if err != nil {
		level = errLevel

		fields = fields[:len(fields):len(fields)]
		fields = append(fields, zap.Error(err))
	}

	logger = logger.WithOptions(zap.AddCallerSkip(2))
	ce := logger.Check(level, msg)
	if ce != nil {
		ce.Write(fields...)
	}
}
