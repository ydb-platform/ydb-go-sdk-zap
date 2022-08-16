package zap

import (
	"go.uber.org/zap"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Scripting returns trace.Scripting with logging events from details
func Scripting(log *zap.Logger, details trace.Details, opts ...option) (t trace.Scripting) {
	if details&trace.ScriptingEvents == 0 {
		return
	}
	options := parseOptions(opts...)
	log = log.Named(`ydb`).Named(`scripting`)
	t.OnExecute = func(info trace.ScriptingExecuteStartInfo) func(trace.ScriptingExecuteDoneInfo) {
		log.Debug(`execute start`)
		start := time.Now()
		return func(info trace.ScriptingExecuteDoneInfo) {
			if info.Error == nil {
				log.Debug(`execute done`,
					zap.Duration("latency", time.Since(start)),
					zap.Int("resultSetCount", info.Result.ResultSetCount()),
					zap.Error(info.Result.Err()),
				)
			} else {
				log.Error(`execute failed`,
					zap.Duration("latency", time.Since(start)),
					zap.Error(info.Error),
					zap.String("version", version),
				)
			}
		}
	}
	t.OnExplain = func(info trace.ScriptingExplainStartInfo) func(trace.ScriptingExplainDoneInfo) {
		log.Debug(`explain start`)
		start := time.Now()
		return func(info trace.ScriptingExplainDoneInfo) {
			if info.Error == nil {
				log.Debug(`explain done`,
					zap.Duration("latency", time.Since(start)),
					zap.String("plan", info.Plan),
				)
			} else {
				log.Error(`explain failed`,
					zap.Duration("latency", time.Since(start)),
					zap.Error(info.Error),
					zap.String("version", version),
				)
			}
		}
	}
	t.OnStreamExecute = func(
		info trace.ScriptingStreamExecuteStartInfo,
	) func(
		trace.ScriptingStreamExecuteIntermediateInfo,
	) func(
		trace.ScriptingStreamExecuteDoneInfo,
	) {
		query := info.Query
		params := info.Parameters
		if options.logQuery {
			log.Debug(`stream execute start`,
				zap.String("query", query),
				zap.Stringer("params", params),
			)
		} else {
			log.Debug(`stream execute start`)
		}
		start := time.Now()
		return func(
			info trace.ScriptingStreamExecuteIntermediateInfo,
		) func(
			trace.ScriptingStreamExecuteDoneInfo,
		) {
			if info.Error == nil {
				log.Debug(`stream execute intermediate`)
			} else {
				log.Warn(`stream execute intermediate failed`,
					zap.Error(info.Error),
					zap.String("version", version),
				)
			}
			return func(info trace.ScriptingStreamExecuteDoneInfo) {
				if info.Error == nil {
					log.Debug(`stream execute done`,
						zap.Duration("latency", time.Since(start)),
						zap.String("query", query),
						zap.Stringer("params", params),
					)
				} else {
					if options.logQuery {
						log.Error(`stream execute failed`,
							zap.Duration("latency", time.Since(start)),
							zap.String("query", query),
							zap.Stringer("params", params),
							zap.Error(info.Error),
							zap.String("version", version),
						)
					} else {
						log.Error(`stream execute failed`,
							zap.Duration("latency", time.Since(start)),
							zap.Error(info.Error),
							zap.String("version", version),
						)
					}
				}
			}
		}
	}
	t.OnClose = func(info trace.ScriptingCloseStartInfo) func(trace.ScriptingCloseDoneInfo) {
		log.Debug(`close start`)
		start := time.Now()
		return func(info trace.ScriptingCloseDoneInfo) {
			if info.Error == nil {
				log.Debug(`close done`,
					zap.Duration("latency", time.Since(start)),
				)
			} else {
				log.Error(`close failed`,
					zap.Duration("latency", time.Since(start)),
					zap.Error(info.Error),
					zap.String("version", version),
				)
			}
		}
	}
	return t
}
