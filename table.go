package zap

import (
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"time"

	"go.uber.org/zap"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Table makes trace.Table with zap logging
func Table(log *zap.Logger, d detailer, opts ...option) (t trace.Table) {
	if d.Details()&trace.TableEvents == 0 {
		return t
	}
	options := parseOptions(opts...)
	log = log.Named("ydb").Named("table")
	if d.Details()&trace.TableEvents != 0 {
		t.OnInit = func(info trace.TableInitStartInfo) func(trace.TableInitDoneInfo) {
			log.Info("initializing")
			start := time.Now()
			return func(info trace.TableInitDoneInfo) {
				log.Info("initialized",
					zap.String("version", version),
					zap.Duration("latency", time.Since(start)),
					zap.Int("limit", info.Limit),
				)
			}
		}
		t.OnClose = func(info trace.TableCloseStartInfo) func(trace.TableCloseDoneInfo) {
			log.Info("closing")
			start := time.Now()
			return func(info trace.TableCloseDoneInfo) {
				if info.Error == nil {
					log.Info("closed",
						zap.Duration("latency", time.Since(start)),
					)
				} else {
					log.Error("close failed",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.Error(info.Error),
					)
				}
			}
		}
	}
	if d.Details()&trace.TableEvents != 0 {
		do := log.Named("do")
		doTx := log.Named("doTx")
		createSession := log.Named("createSession")
		t.OnCreateSession = func(info trace.TableCreateSessionStartInfo) func(info trace.TableCreateSessionIntermediateInfo) func(trace.TableCreateSessionDoneInfo) {
			createSession.Debug("creating session")
			start := time.Now()
			return func(info trace.TableCreateSessionIntermediateInfo) func(trace.TableCreateSessionDoneInfo) {
				if info.Error == nil {
					createSession.Debug("intermediate",
						zap.Duration("latency", time.Since(start)),
					)
				} else {
					createSession.Warn("intermediate",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.Error(info.Error),
					)
				}
				return func(info trace.TableCreateSessionDoneInfo) {
					if info.Error == nil {
						createSession.Debug("finish",
							zap.Duration("latency", time.Since(start)),
							zap.Int("attempts", info.Attempts),
							zap.String("id", info.Session.ID()),
							zap.String("status", info.Session.Status()),
						)
					} else {
						createSession.Error("finish",
							zap.String("version", version),
							zap.Duration("latency", time.Since(start)),
							zap.Int("attempts", info.Attempts),
							zap.Error(info.Error),
						)
					}
				}
			}
		}
		t.OnDo = func(info trace.TableDoStartInfo) func(info trace.TableDoIntermediateInfo) func(trace.TableDoDoneInfo) {
			idempotent := info.Idempotent
			if info.NestedCall {
				do.Error("nested call")
			}
			do.Debug("init",
				zap.Bool("idempotent", idempotent),
			)
			start := time.Now()
			return func(info trace.TableDoIntermediateInfo) func(trace.TableDoDoneInfo) {
				if info.Error == nil {
					do.Debug("attempt",
						zap.Duration("latency", time.Since(start)),
						zap.Bool("idempotent", idempotent),
					)
				} else {
					f := do.Warn
					if !ydb.IsYdbError(info.Error) {
						f = do.Debug
					}
					m := retry.Check(info.Error)
					f("intermediate",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.Bool("idempotent", idempotent),
						zap.Bool("retryable", m.MustRetry(idempotent)),
						zap.Bool("deleteSession", m.MustDeleteSession()),
						zap.Int64("code", m.StatusCode()),
						zap.Error(info.Error),
					)
				}
				return func(info trace.TableDoDoneInfo) {
					if info.Error == nil {
						do.Debug("finish",
							zap.Duration("latency", time.Since(start)),
							zap.Bool("idempotent", idempotent),
							zap.Int("attempts", info.Attempts),
						)
					} else {
						f := do.Error
						if !ydb.IsYdbError(info.Error) {
							f = do.Debug
						}
						m := retry.Check(info.Error)
						f("done",
							zap.String("version", version),
							zap.Duration("latency", time.Since(start)),
							zap.Bool("idempotent", idempotent),
							zap.Bool("retryable", m.MustRetry(idempotent)),
							zap.Bool("deleteSession", m.MustDeleteSession()),
							zap.Int64("code", m.StatusCode()),
							zap.Error(info.Error),
						)
					}
				}
			}
		}
		t.OnDoTx = func(info trace.TableDoTxStartInfo) func(info trace.TableDoTxIntermediateInfo) func(trace.TableDoTxDoneInfo) {
			idempotent := info.Idempotent
			if info.NestedCall {
				do.Error("nested call")
			}
			doTx.Debug("init",
				zap.Bool("idempotent", idempotent),
			)
			start := time.Now()
			return func(info trace.TableDoTxIntermediateInfo) func(trace.TableDoTxDoneInfo) {
				if info.Error == nil {
					doTx.Debug("attempt",
						zap.Duration("latency", time.Since(start)),
						zap.Bool("idempotent", idempotent),
					)
				} else {
					f := doTx.Warn
					if !ydb.IsYdbError(info.Error) {
						f = doTx.Debug
					}
					m := retry.Check(info.Error)
					f("intermediate",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.Bool("idempotent", idempotent),
						zap.Bool("retryable", m.MustRetry(idempotent)),
						zap.Bool("deleteSession", m.MustDeleteSession()),
						zap.Int64("code", m.StatusCode()),
						zap.Error(info.Error),
					)
				}
				return func(info trace.TableDoTxDoneInfo) {
					if info.Error == nil {
						doTx.Debug("finish",
							zap.Duration("latency", time.Since(start)),
							zap.Bool("idempotent", idempotent),
							zap.Int("attempts", info.Attempts),
						)
					} else {
						f := doTx.Error
						if !ydb.IsYdbError(info.Error) {
							f = doTx.Debug
						}
						m := retry.Check(info.Error)
						f("done",
							zap.String("version", version),
							zap.Duration("latency", time.Since(start)),
							zap.Bool("idempotent", idempotent),
							zap.Bool("retryable", m.MustRetry(idempotent)),
							zap.Bool("deleteSession", m.MustDeleteSession()),
							zap.Int64("code", m.StatusCode()),
							zap.Error(info.Error),
						)
					}
				}
			}
		}
	}
	if d.Details()&trace.TableSessionEvents != 0 {
		log := log.Named("session")
		if d.Details()&trace.TableSessionLifeCycleEvents != 0 {
			t.OnSessionNew = func(info trace.TableSessionNewStartInfo) func(trace.TableSessionNewDoneInfo) {
				log.Debug("try to create")
				start := time.Now()
				return func(info trace.TableSessionNewDoneInfo) {
					if info.Error == nil {
						log.Info("created",
							zap.Duration("latency", time.Since(start)),
							zap.String("id", info.Session.ID()),
							zap.String("status", info.Session.Status()),
						)
					} else {
						log.Error("create failed",
							zap.String("version", version),
							zap.Duration("latency", time.Since(start)),
							zap.Error(info.Error),
						)
					}
				}
			}
			t.OnSessionDelete = func(info trace.TableSessionDeleteStartInfo) func(trace.TableSessionDeleteDoneInfo) {
				session := info.Session
				log.Debug("try to delete",
					zap.String("id", session.ID()),
					zap.String("status", session.Status()),
				)
				start := time.Now()
				return func(info trace.TableSessionDeleteDoneInfo) {
					if info.Error == nil {
						log.Debug("deleted",
							zap.Duration("latency", time.Since(start)),
							zap.String("id", session.ID()),
							zap.String("status", session.Status()),
						)
					} else {
						log.Warn("delete failed",
							zap.String("version", version),
							zap.Duration("latency", time.Since(start)),
							zap.String("id", session.ID()),
							zap.String("status", session.Status()),
							zap.Error(info.Error),
						)
					}
				}
			}
			t.OnSessionKeepAlive = func(info trace.TableKeepAliveStartInfo) func(trace.TableKeepAliveDoneInfo) {
				session := info.Session
				log.Debug("keep-aliving",
					zap.String("id", session.ID()),
					zap.String("status", session.Status()),
				)
				start := time.Now()
				return func(info trace.TableKeepAliveDoneInfo) {
					if info.Error == nil {
						log.Debug("keep-alived",
							zap.Duration("latency", time.Since(start)),
							zap.String("id", session.ID()),
							zap.String("status", session.Status()),
						)
					} else {
						log.Warn("keep-alive failed",
							zap.String("version", version),
							zap.Duration("latency", time.Since(start)),
							zap.String("id", session.ID()),
							zap.String("status", session.Status()),
							zap.Error(info.Error),
						)
					}
				}
			}
		}
		if d.Details()&trace.TableSessionQueryEvents != 0 {
			log := log.Named("query")
			if d.Details()&trace.TableSessionQueryInvokeEvents != 0 {
				log := log.Named("invoke")
				t.OnSessionQueryPrepare = func(
					info trace.TablePrepareDataQueryStartInfo,
				) func(
					trace.TablePrepareDataQueryDoneInfo,
				) {
					session := info.Session
					query := info.Query
					if options.logQuery {
						log.Debug("preparing",
							zap.String("id", session.ID()),
							zap.String("status", session.Status()),
							zap.String("query", query),
						)
					} else {
						log.Debug("preparing",
							zap.String("id", session.ID()),
							zap.String("status", session.Status()),
						)
					}
					start := time.Now()
					return func(info trace.TablePrepareDataQueryDoneInfo) {
						if info.Error == nil {
							if options.logQuery {
								log.Debug(
									"prepared",
									zap.Duration("latency", time.Since(start)),
									zap.String("id", session.ID()),
									zap.String("status", session.Status()),
									zap.String("query", query),
									zap.String("yql", info.Result.String()),
								)
							} else {
								log.Debug(
									"prepared",
									zap.Duration("latency", time.Since(start)),
									zap.String("id", session.ID()),
									zap.String("status", session.Status()),
									zap.String("yql", info.Result.String()),
								)
							}
						} else {
							if options.logQuery {
								log.Error(
									"prepare failed",
									zap.String("version", version),
									zap.Duration("latency", time.Since(start)),
									zap.String("id", session.ID()),
									zap.String("status", session.Status()),
									zap.String("query", query),
									zap.Error(info.Error),
								)
							} else {
								log.Error(
									"prepare failed",
									zap.String("version", version),
									zap.Duration("latency", time.Since(start)),
									zap.String("id", session.ID()),
									zap.String("status", session.Status()),
									zap.Error(info.Error),
								)
							}
						}
					}
				}
				t.OnSessionQueryExecute = func(
					info trace.TableExecuteDataQueryStartInfo,
				) func(
					trace.TableExecuteDataQueryDoneInfo,
				) {
					session := info.Session
					query := info.Query
					params := info.Parameters
					if options.logQuery {
						log.Debug("executing",
							zap.String("id", session.ID()),
							zap.String("status", session.Status()),
							zap.String("yql", query.String()),
							zap.String("params", params.String()),
							zap.Bool("keepInCache", info.KeepInCache),
						)
					} else {
						log.Debug("executing",
							zap.String("id", session.ID()),
							zap.String("status", session.Status()),
							zap.String("params", params.String()),
							zap.Bool("keepInCache", info.KeepInCache),
						)
					}
					start := time.Now()
					return func(info trace.TableExecuteDataQueryDoneInfo) {
						if info.Error == nil {
							tx := info.Tx
							if options.logQuery {
								log.Debug("executed",
									zap.Duration("latency", time.Since(start)),
									zap.String("id", session.ID()),
									zap.String("status", session.Status()),
									zap.String("tx", tx.ID()),
									zap.String("yql", query.String()),
									zap.String("params", params.String()),
									zap.Bool("prepared", info.Prepared),
									zap.NamedError("resultErr", info.Result.Err()),
								)
							} else {
								log.Debug("executed",
									zap.Duration("latency", time.Since(start)),
									zap.String("id", session.ID()),
									zap.String("status", session.Status()),
									zap.String("tx", tx.ID()),
									zap.String("params", params.String()),
									zap.Bool("prepared", info.Prepared),
									zap.NamedError("resultErr", info.Result.Err()),
								)
							}
						} else {
							if options.logQuery {
								log.Error("execute failed",
									zap.String("version", version),
									zap.Duration("latency", time.Since(start)),
									zap.String("id", session.ID()),
									zap.String("status", session.Status()),
									zap.String("yql", query.String()),
									zap.String("params", params.String()),
									zap.Bool("prepared", info.Prepared),
									zap.Error(info.Error),
								)
							} else {
								log.Error("execute failed",
									zap.String("version", version),
									zap.Duration("latency", time.Since(start)),
									zap.String("id", session.ID()),
									zap.String("status", session.Status()),
									zap.String("params", params.String()),
									zap.Bool("prepared", info.Prepared),
									zap.Error(info.Error),
								)
							}
						}
					}
				}
			}
			if d.Details()&trace.TableSessionQueryStreamEvents != 0 {
				log := log.Named("stream")
				t.OnSessionQueryStreamExecute = func(
					info trace.TableSessionQueryStreamExecuteStartInfo,
				) func(
					intermediateInfo trace.TableSessionQueryStreamExecuteIntermediateInfo,
				) func(
					trace.TableSessionQueryStreamExecuteDoneInfo,
				) {
					session := info.Session
					query := info.Query
					params := info.Parameters
					if options.logQuery {
						log.Debug("executing",
							zap.String("id", session.ID()),
							zap.String("status", session.Status()),
							zap.String("yql", query.String()),
							zap.String("params", params.String()),
						)
					} else {
						log.Debug("executing",
							zap.String("id", session.ID()),
							zap.String("status", session.Status()),
							zap.String("params", params.String()),
						)
					}
					start := time.Now()
					return func(
						info trace.TableSessionQueryStreamExecuteIntermediateInfo,
					) func(
						trace.TableSessionQueryStreamExecuteDoneInfo,
					) {
						if info.Error == nil {
							log.Debug(`intermediate`)
						} else {
							log.Error(`intermediate failed`,
								zap.Error(info.Error),
								zap.String("version", version),
							)
						}
						return func(info trace.TableSessionQueryStreamExecuteDoneInfo) {
							if info.Error == nil {
								if options.logQuery {
									log.Debug("executed",
										zap.Duration("latency", time.Since(start)),
										zap.String("id", session.ID()),
										zap.String("status", session.Status()),
										zap.String("yql", query.String()),
										zap.String("params", params.String()),
										zap.Error(info.Error),
									)
								} else {
									log.Debug("executed",
										zap.Duration("latency", time.Since(start)),
										zap.String("id", session.ID()),
										zap.String("status", session.Status()),
										zap.String("params", params.String()),
										zap.Error(info.Error),
									)
								}
							} else {
								if options.logQuery {
									log.Error("execute failed",
										zap.String("version", version),
										zap.Duration("latency", time.Since(start)),
										zap.String("id", session.ID()),
										zap.String("status", session.Status()),
										zap.String("yql", query.String()),
										zap.String("params", params.String()),
										zap.Error(info.Error),
									)
								} else {
									log.Error("execute failed",
										zap.String("version", version),
										zap.Duration("latency", time.Since(start)),
										zap.String("id", session.ID()),
										zap.String("status", session.Status()),
										zap.String("params", params.String()),
										zap.Error(info.Error),
									)
								}
							}
						}
					}
				}
				t.OnSessionQueryStreamRead = func(
					info trace.TableSessionQueryStreamReadStartInfo,
				) func(
					trace.TableSessionQueryStreamReadIntermediateInfo,
				) func(
					trace.TableSessionQueryStreamReadDoneInfo,
				) {
					session := info.Session
					log.Debug("reading",
						zap.String("id", session.ID()),
						zap.String("status", session.Status()),
					)
					start := time.Now()
					return func(
						info trace.TableSessionQueryStreamReadIntermediateInfo,
					) func(
						trace.TableSessionQueryStreamReadDoneInfo,
					) {
						if info.Error == nil {
							log.Debug(`intermediate`)
						} else {
							log.Error(`intermediate failed`,
								zap.String("version", version),
								zap.Error(info.Error),
							)
						}
						return func(info trace.TableSessionQueryStreamReadDoneInfo) {
							if info.Error == nil {
								log.Debug("read",
									zap.Duration("latency", time.Since(start)),
									zap.String("id", session.ID()),
									zap.String("status", session.Status()),
								)
							} else {
								log.Error("read failed",
									zap.String("version", version),
									zap.Duration("latency", time.Since(start)),
									zap.String("id", session.ID()),
									zap.String("status", session.Status()),
									zap.Error(info.Error),
								)
							}
						}
					}
				}
			}
		}
		if d.Details()&trace.TableSessionTransactionEvents != 0 {
			log := log.Named("transaction")
			t.OnSessionTransactionBegin = func(info trace.TableSessionTransactionBeginStartInfo) func(trace.TableSessionTransactionBeginDoneInfo) {
				session := info.Session
				log.Debug("beginning",
					zap.String("id", session.ID()),
					zap.String("status", session.Status()),
				)
				start := time.Now()
				return func(info trace.TableSessionTransactionBeginDoneInfo) {
					if info.Error == nil {
						log.Debug("began",
							zap.Duration("latency", time.Since(start)),
							zap.String("id", session.ID()),
							zap.String("status", session.Status()),
							zap.String("tx", info.Tx.ID()),
						)
					} else {
						log.Debug("begin failed",
							zap.String("version", version),
							zap.Duration("latency", time.Since(start)),
							zap.String("id", session.ID()),
							zap.String("status", session.Status()),
							zap.Error(info.Error),
						)
					}
				}
			}
			t.OnSessionTransactionCommit = func(info trace.TableSessionTransactionCommitStartInfo) func(trace.TableSessionTransactionCommitDoneInfo) {
				session := info.Session
				tx := info.Tx
				log.Debug("committing",
					zap.String("id", session.ID()),
					zap.String("status", session.Status()),
					zap.String("tx", tx.ID()),
				)
				start := time.Now()
				return func(info trace.TableSessionTransactionCommitDoneInfo) {
					if info.Error == nil {
						log.Debug("committed",
							zap.Duration("latency", time.Since(start)),
							zap.String("id", session.ID()),
							zap.String("status", session.Status()),
							zap.String("tx", tx.ID()),
						)
					} else {
						log.Debug("commit failed",
							zap.String("version", version),
							zap.Duration("latency", time.Since(start)),
							zap.String("id", session.ID()),
							zap.String("status", session.Status()),
							zap.String("tx", tx.ID()),
							zap.Error(info.Error),
						)
					}
				}
			}
			t.OnSessionTransactionRollback = func(info trace.TableSessionTransactionRollbackStartInfo) func(trace.TableSessionTransactionRollbackDoneInfo) {
				session := info.Session
				tx := info.Tx
				log.Debug("try to rollback",
					zap.String("id", session.ID()),
					zap.String("status", session.Status()),
					zap.String("tx", tx.ID()),
				)
				start := time.Now()
				return func(info trace.TableSessionTransactionRollbackDoneInfo) {
					if info.Error == nil {
						log.Debug("rollback done",
							zap.Duration("latency", time.Since(start)),
							zap.String("id", session.ID()),
							zap.String("status", session.Status()),
							zap.String("tx", tx.ID()),
						)
					} else {
						log.Error("rollback failed",
							zap.String("version", version),
							zap.Duration("latency", time.Since(start)),
							zap.String("id", session.ID()),
							zap.String("status", session.Status()),
							zap.String("tx", tx.ID()),
							zap.Error(info.Error),
						)
					}
				}
			}
		}
	}
	if d.Details()&trace.TablePoolEvents != 0 {
		log := log.Named("pool")
		if d.Details()&trace.TablePoolSessionLifeCycleEvents != 0 {
			log := log.Named("session")
			t.OnPoolSessionAdd = func(info trace.TablePoolSessionAddInfo) {
				log.Debug("session added to pool",
					zap.String("id", info.Session.ID()),
					zap.String("status", info.Session.Status()),
				)
			}
			t.OnPoolSessionRemove = func(info trace.TablePoolSessionRemoveInfo) {
				log.Debug("session removed from pool",
					zap.String("id", info.Session.ID()),
					zap.String("status", info.Session.Status()),
				)
			}
			t.OnPoolStateChange = func(info trace.TablePoolStateChangeInfo) {
				log.Debug("change",
					zap.Int("size", info.Size),
					zap.String("event", info.Event),
				)
			}
		}
		if d.Details()&trace.TablePoolAPIEvents != 0 {
			t.OnPoolPut = func(info trace.TablePoolPutStartInfo) func(trace.TablePoolPutDoneInfo) {
				session := info.Session
				log.Debug("putting",
					zap.String("id", session.ID()),
					zap.String("status", session.Status()),
				)
				start := time.Now()
				return func(info trace.TablePoolPutDoneInfo) {
					if info.Error == nil {
						log.Debug("put",
							zap.Duration("latency", time.Since(start)),
							zap.String("id", session.ID()),
							zap.String("status", session.Status()),
						)
					} else {
						log.Error("put failed",
							zap.String("version", version),
							zap.Duration("latency", time.Since(start)),
							zap.String("id", session.ID()),
							zap.String("status", session.Status()),
							zap.Error(info.Error),
						)
					}
				}
			}
			t.OnPoolGet = func(info trace.TablePoolGetStartInfo) func(trace.TablePoolGetDoneInfo) {
				log.Debug("getting")
				start := time.Now()
				return func(info trace.TablePoolGetDoneInfo) {
					if info.Error == nil {
						session := info.Session
						log.Debug("got",
							zap.Duration("latency", time.Since(start)),
							zap.String("id", session.ID()),
							zap.String("status", session.Status()),
							zap.Int("attempts", info.Attempts),
						)
					} else {
						log.Warn("get failed",
							zap.String("version", version),
							zap.Duration("latency", time.Since(start)),
							zap.Int("attempts", info.Attempts),
							zap.Error(info.Error),
						)
					}
				}
			}
			t.OnPoolWait = func(info trace.TablePoolWaitStartInfo) func(trace.TablePoolWaitDoneInfo) {
				log.Debug("waiting")
				start := time.Now()
				return func(info trace.TablePoolWaitDoneInfo) {
					if info.Error == nil {
						if info.Session == nil {
							log.Debug(`wait done without any significant result`,
								zap.Duration("latency", time.Since(start)),
							)
						} else {
							log.Debug(`wait done`,
								zap.Duration("latency", time.Since(start)),
								zap.String("id", info.Session.ID()),
								zap.String("status", info.Session.Status()),
							)
						}
					} else {
						if info.Session == nil {
							log.Debug(`wait failed without any significant result`,
								zap.Duration("latency", time.Since(start)),
								zap.Error(info.Error),
							)
						} else {
							log.Warn(`wait failed`,
								zap.String("version", version),
								zap.Duration("latency", time.Since(start)),
								zap.String("id", info.Session.ID()),
								zap.String("status", info.Session.Status()),
								zap.Error(info.Error),
							)
						}
					}
				}
			}
		}
	}
	return t
}
