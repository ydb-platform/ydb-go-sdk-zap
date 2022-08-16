package zap

import (
	"go.uber.org/zap"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// DatabaseSQL makes trace.DatabaseSQL with logging events from details
func DatabaseSQL(log *zap.Logger, details trace.Details, opts ...option) (t trace.DatabaseSQL) {
	if details&trace.DatabaseSQLEvents == 0 {
		return
	}
	options := parseOptions(opts...)
	log = log.Named(`ydb`).Named(`database`).Named(`sql`)
	if details&trace.DatabaseSQLConnectorEvents != 0 {
		//nolint:govet
		log := log.Named(`connector`)
		t.OnConnectorConnect = func(
			info trace.DatabaseSQLConnectorConnectStartInfo,
		) func(
			trace.DatabaseSQLConnectorConnectDoneInfo,
		) {
			log.Debug("connect start")
			start := time.Now()
			return func(info trace.DatabaseSQLConnectorConnectDoneInfo) {
				if info.Error == nil {
					log.Info(`connected`,
						zap.Duration("latency", time.Since(start)),
					)
				} else {
					log.Error(`connect failed`,
						zap.Duration("latency", time.Since(start)),
						zap.Error(info.Error),
						zap.String("version", version),
					)
				}
			}
		}
	}
	//nolint:nestif
	if details&trace.DatabaseSQLConnEvents != 0 {
		//nolint:govet
		log := log.Named(`conn`)
		t.OnConnPing = func(info trace.DatabaseSQLConnPingStartInfo) func(trace.DatabaseSQLConnPingDoneInfo) {
			log.Debug("ping start")
			start := time.Now()
			return func(info trace.DatabaseSQLConnPingDoneInfo) {
				if info.Error == nil {
					log.Debug(`ping done`,
						zap.Duration("latency", time.Since(start)),
					)
				} else {
					log.Error(`ping failed`,
						zap.Duration("latency", time.Since(start)),
						zap.Error(info.Error),
						zap.String("version", version),
					)
				}
			}
		}
		t.OnConnClose = func(info trace.DatabaseSQLConnCloseStartInfo) func(trace.DatabaseSQLConnCloseDoneInfo) {
			log.Debug("close start")
			start := time.Now()
			return func(info trace.DatabaseSQLConnCloseDoneInfo) {
				if info.Error == nil {
					log.Info(`closed`,
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
		t.OnConnBegin = func(info trace.DatabaseSQLConnBeginStartInfo) func(trace.DatabaseSQLConnBeginDoneInfo) {
			log.Debug("begin transaction start")
			start := time.Now()
			return func(info trace.DatabaseSQLConnBeginDoneInfo) {
				if info.Error == nil {
					log.Debug(`begin transaction was success`,
						zap.Duration("latency", time.Since(start)),
					)
				} else {
					log.Error(`begin transaction failed`,
						zap.Duration("latency", time.Since(start)),
						zap.Error(info.Error),
						zap.String("version", version),
					)
				}
			}
		}
		t.OnConnPrepare = func(info trace.DatabaseSQLConnPrepareStartInfo) func(trace.DatabaseSQLConnPrepareDoneInfo) {
			if options.logQuery {
				log.Debug("prepare statement start {query:\"%s\"}",
					zap.String("query", info.Query),
				)
			} else {
				log.Debug("prepare statement start")
			}
			query := info.Query
			start := time.Now()
			return func(info trace.DatabaseSQLConnPrepareDoneInfo) {
				if info.Error == nil {
					log.Debug(`prepare statement was success`,
						zap.Duration("latency", time.Since(start)),
					)
				} else {
					if options.logQuery {
						log.Error(`prepare statement failed`,
							zap.Duration("latency", time.Since(start)),
							zap.String("query", query),
							zap.Error(info.Error),
							zap.String("version", version),
						)
					} else {
						log.Error(`prepare statement failed`,
							zap.Duration("latency", time.Since(start)),
							zap.Error(info.Error),
							zap.String("version", version),
						)
					}
				}
			}
		}
		t.OnConnExec = func(info trace.DatabaseSQLConnExecStartInfo) func(trace.DatabaseSQLConnExecDoneInfo) {
			if options.logQuery {
				log.Debug("exec start {query:\"%s\"}",
					zap.String("query", info.Query),
				)
			} else {
				log.Debug("exec start")
			}
			query := info.Query
			idempotent := info.Idempotent
			start := time.Now()
			return func(info trace.DatabaseSQLConnExecDoneInfo) {
				if info.Error == nil {
					log.Debug(`exec was success`,
						zap.Duration("latency", time.Since(start)),
					)
				} else {
					m := retry.Check(info.Error)
					if options.logQuery {
						log.Error(`exec failed`,
							zap.Duration("latency", time.Since(start)),
							zap.String("query", query),
							zap.Error(info.Error),
							zap.Bool("retryable", m.MustRetry(idempotent)),
							zap.Int64("code", m.StatusCode()),
							zap.Bool("deleteSession", m.MustDeleteSession()),
							zap.String("version", version),
						)
					} else {
						log.Error(`exec failed`,
							zap.Duration("latency", time.Since(start)),
							zap.Error(info.Error),
							zap.Bool("retryable", m.MustRetry(idempotent)),
							zap.Int64("code", m.StatusCode()),
							zap.Bool("deleteSession", m.MustDeleteSession()),
							zap.String("version", version),
						)
					}
				}
			}
		}
		t.OnConnQuery = func(info trace.DatabaseSQLConnQueryStartInfo) func(trace.DatabaseSQLConnQueryDoneInfo) {
			if options.logQuery {
				log.Debug("query start {query:\"%s\"}",
					zap.String("query", info.Query),
				)
			} else {
				log.Debug("query start")
			}
			query := info.Query
			idempotent := info.Idempotent
			start := time.Now()
			return func(info trace.DatabaseSQLConnQueryDoneInfo) {
				if info.Error == nil {
					log.Debug(`query was success`,
						zap.Duration("latency", time.Since(start)),
					)
				} else {
					m := retry.Check(info.Error)
					if options.logQuery {
						log.Error(`exec failed`,
							zap.Duration("latency", time.Since(start)),
							zap.String("query", query),
							zap.Error(info.Error),
							zap.Bool("retryable", m.MustRetry(idempotent)),
							zap.Int64("code", m.StatusCode()),
							zap.Bool("deleteSession", m.MustDeleteSession()),
							zap.String("version", version),
						)
					} else {
						log.Error(`exec failed`,
							zap.Duration("latency", time.Since(start)),
							zap.Error(info.Error),
							zap.Bool("retryable", m.MustRetry(idempotent)),
							zap.Int64("code", m.StatusCode()),
							zap.Bool("deleteSession", m.MustDeleteSession()),
							zap.String("version", version),
						)
					}
				}
			}
		}
	}
	if details&trace.DatabaseSQLTxEvents != 0 {
		//nolint:govet
		log := log.Named(`tx`)
		t.OnTxCommit = func(info trace.DatabaseSQLTxCommitStartInfo) func(trace.DatabaseSQLTxCommitDoneInfo) {
			log.Debug("commit start")
			start := time.Now()
			return func(info trace.DatabaseSQLTxCommitDoneInfo) {
				if info.Error == nil {
					log.Debug(`committed`,
						zap.Duration("latency", time.Since(start)),
					)
				} else {
					log.Error(`commit failed`,
						zap.Duration("latency", time.Since(start)),
						zap.Error(info.Error),
						zap.String("version", version),
					)
				}
			}
		}
		t.OnTxRollback = func(info trace.DatabaseSQLTxRollbackStartInfo) func(trace.DatabaseSQLTxRollbackDoneInfo) {
			log.Debug("rollback start")
			start := time.Now()
			return func(info trace.DatabaseSQLTxRollbackDoneInfo) {
				if info.Error == nil {
					log.Debug(`rollbacked`,
						zap.Duration("latency", time.Since(start)),
					)
				} else {
					log.Error(`rollback failed`,
						zap.Duration("latency", time.Since(start)),
						zap.Error(info.Error),
						zap.String("version", version),
					)
				}
			}
		}
	}
	//nolint:nestif
	if details&trace.DatabaseSQLStmtEvents != 0 {
		//nolint:govet
		log := log.Named(`stmt`)
		t.OnStmtClose = func(info trace.DatabaseSQLStmtCloseStartInfo) func(trace.DatabaseSQLStmtCloseDoneInfo) {
			log.Debug("close start")
			start := time.Now()
			return func(info trace.DatabaseSQLStmtCloseDoneInfo) {
				if info.Error == nil {
					log.Debug(`closed`,
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
		t.OnStmtExec = func(info trace.DatabaseSQLStmtExecStartInfo) func(trace.DatabaseSQLStmtExecDoneInfo) {
			if options.logQuery {
				log.Debug("exec start {query:\"%s\"}",
					zap.String("query", info.Query),
				)
			} else {
				log.Debug("exec start")
			}
			query := info.Query
			start := time.Now()
			return func(info trace.DatabaseSQLStmtExecDoneInfo) {
				if info.Error == nil {
					log.Debug(`exec was success`,
						zap.Duration("latency", time.Since(start)),
					)
				} else {
					if options.logQuery {
						log.Error(`exec failed`,
							zap.Duration("latency", time.Since(start)),
							zap.String("query", query),
							zap.Error(info.Error),
							zap.String("version", version),
						)
					} else {
						log.Error(`exec failed`,
							zap.Duration("latency", time.Since(start)),
							zap.Error(info.Error),
							zap.String("version", version),
						)
					}
				}
			}
		}
		t.OnStmtQuery = func(info trace.DatabaseSQLStmtQueryStartInfo) func(trace.DatabaseSQLStmtQueryDoneInfo) {
			if options.logQuery {
				log.Debug("query start {query:\"%s\"}",
					zap.String("query", info.Query),
				)
			} else {
				log.Debug("query start")
			}
			query := info.Query
			start := time.Now()
			return func(info trace.DatabaseSQLStmtQueryDoneInfo) {
				if info.Error == nil {
					log.Debug(`query was success`,
						zap.Duration("latency", time.Since(start)),
					)
				} else {
					if options.logQuery {
						log.Error(`query failed`,
							zap.Duration("latency", time.Since(start)),
							zap.String("query", query),
							zap.Error(info.Error),
							zap.String("version", version),
						)
					} else {
						log.Error(`query failed`,
							zap.Duration("latency", time.Since(start)),
							zap.Error(info.Error),
							zap.String("version", version),
						)
					}
				}
			}
		}
	}
	return t
}
