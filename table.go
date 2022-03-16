package zap

import (
	"time"

	"go.uber.org/zap"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Table makes trace.Table with zap logging
func Table(log *zap.Logger, details trace.Details) trace.Table {
	log = log.Named("ydb").Named("table")
	t := trace.Table{}
	if details&trace.TableEvents != 0 {
		t.OnInit = func(info trace.TableInitStartInfo) func(trace.TableInitDoneInfo) {
			log.Info("initializing",
				zap.String("version", version),
			)
			start := time.Now()
			return func(info trace.TableInitDoneInfo) {
				log.Info("initialized",
					zap.String("version", version),
					zap.Duration("latency", time.Since(start)),
					zap.Int("minSize", info.KeepAliveMinSize),
					zap.Int("maxSize", info.Limit),
				)
			}
		}
		t.OnClose = func(info trace.TableCloseStartInfo) func(trace.TableCloseDoneInfo) {
			log.Info("closing",
				zap.String("version", version),
			)
			start := time.Now()
			return func(info trace.TableCloseDoneInfo) {
				if info.Error == nil {
					log.Info("closed",
						zap.String("version", version),
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
	if details&trace.TableEvents != 0 {
		do := log.Named("do")
		doTx := log.Named("doTx")
		createSession := log.Named("createSession")
		t.OnCreateSession = func(info trace.TableCreateSessionStartInfo) func(info trace.TableCreateSessionIntermediateInfo) func(trace.TableCreateSessionDoneInfo) {
			createSession.Debug("init",
				zap.String("version", version),
			)
			start := time.Now()
			return func(info trace.TableCreateSessionIntermediateInfo) func(trace.TableCreateSessionDoneInfo) {
				if info.Error == nil {
					createSession.Debug("intermediate",
						zap.String("version", version),
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
							zap.String("version", version),
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
			do.Debug("init",
				zap.String("version", version),
				zap.Bool("idempotent", idempotent))
			start := time.Now()
			return func(info trace.TableDoIntermediateInfo) func(trace.TableDoDoneInfo) {
				if info.Error == nil {
					do.Debug("intermediate",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.Bool("idempotent", idempotent),
					)
				} else {
					do.Warn("intermediate",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.Bool("idempotent", idempotent),
						zap.Error(info.Error),
					)
				}
				return func(info trace.TableDoDoneInfo) {
					if info.Error == nil {
						do.Debug("finish",
							zap.String("version", version),
							zap.Duration("latency", time.Since(start)),
							zap.Bool("idempotent", idempotent),
							zap.Int("attempts", info.Attempts),
						)
					} else {
						do.Error("finish",
							zap.String("version", version),
							zap.Duration("latency", time.Since(start)),
							zap.Bool("idempotent", idempotent),
							zap.Int("attempts", info.Attempts),
							zap.Error(info.Error),
						)
					}
				}
			}
		}
		t.OnDoTx = func(info trace.TableDoTxStartInfo) func(info trace.TableDoTxIntermediateInfo) func(trace.TableDoTxDoneInfo) {
			idempotent := info.Idempotent
			doTx.Debug("init",
				zap.String("version", version),
				zap.Bool("idempotent", idempotent))
			start := time.Now()
			return func(info trace.TableDoTxIntermediateInfo) func(trace.TableDoTxDoneInfo) {
				if info.Error == nil {
					doTx.Debug("intermediate",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.Bool("idempotent", idempotent),
					)
				} else {
					doTx.Warn("intermediate",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.Bool("idempotent", idempotent),
						zap.Error(info.Error),
					)
				}
				return func(info trace.TableDoTxDoneInfo) {
					if info.Error == nil {
						doTx.Debug("finish",
							zap.String("version", version),
							zap.Duration("latency", time.Since(start)),
							zap.Bool("idempotent", idempotent),
							zap.Int("attempts", info.Attempts),
						)
					} else {
						doTx.Error("finish",
							zap.String("version", version),
							zap.Duration("latency", time.Since(start)),
							zap.Bool("idempotent", idempotent),
							zap.Int("attempts", info.Attempts),
							zap.Error(info.Error),
						)
					}
				}
			}
		}
	}
	if details&trace.TableSessionEvents != 0 {
		log := log.Named("session")
		if details&trace.TableSessionLifeCycleEvents != 0 {
			t.OnSessionNew = func(info trace.TableSessionNewStartInfo) func(trace.TableSessionNewDoneInfo) {
				log.Debug("try to create",
					zap.String("version", version),
				)
				start := time.Now()
				return func(info trace.TableSessionNewDoneInfo) {
					if info.Error == nil {
						log.Info("created",
							zap.String("version", version),
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
					zap.String("version", version),
					zap.String("id", session.ID()),
					zap.String("status", session.Status()),
				)
				start := time.Now()
				return func(info trace.TableSessionDeleteDoneInfo) {
					if info.Error == nil {
						log.Debug("deleted",
							zap.String("version", version),
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
					zap.String("version", version),
					zap.String("id", session.ID()),
					zap.String("status", session.Status()),
				)
				start := time.Now()
				return func(info trace.TableKeepAliveDoneInfo) {
					if info.Error == nil {
						log.Debug("keep-alived",
							zap.String("version", version),
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
		if details&trace.TableSessionQueryEvents != 0 {
			log := log.Named("query")
			if details&trace.TableSessionQueryInvokeEvents != 0 {
				log := log.Named("invoke")
				t.OnSessionQueryPrepare = func(
					info trace.TablePrepareDataQueryStartInfo,
				) func(
					trace.TablePrepareDataQueryDoneInfo,
				) {
					session := info.Session
					query := info.Query
					log.Debug("preparing",
						zap.String("version", version),
						zap.String("id", session.ID()),
						zap.String("status", session.Status()),
						zap.String("query", query),
					)
					start := time.Now()
					return func(info trace.TablePrepareDataQueryDoneInfo) {
						if info.Error == nil {
							log.Debug(
								"prepared",
								zap.String("version", version),
								zap.Duration("latency", time.Since(start)),
								zap.String("id", session.ID()),
								zap.String("status", session.Status()),
								zap.String("query", query),
								zap.String("yql", info.Result.String()),
							)
						} else {
							log.Error(
								"prepare failed",
								zap.String("version", version),
								zap.Duration("latency", time.Since(start)),
								zap.String("id", session.ID()),
								zap.String("status", session.Status()),
								zap.String("query", query),
								zap.Error(info.Error),
							)
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
					log.Debug("executing",
						zap.String("version", version),
						zap.String("id", session.ID()),
						zap.String("status", session.Status()),
						zap.String("yql", query.String()),
						zap.String("params", params.String()),
					)
					start := time.Now()
					return func(info trace.TableExecuteDataQueryDoneInfo) {
						if info.Error == nil {
							tx := info.Tx
							log.Debug("executed",
								zap.String("version", version),
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
						}
					}
				}
			}
			if details&trace.TableSessionQueryStreamEvents != 0 {
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
					log.Debug("executing",
						zap.String("version", version),
						zap.String("id", session.ID()),
						zap.String("status", session.Status()),
						zap.String("yql", query.String()),
						zap.String("params", params.String()),
					)
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
							)
						}
						return func(info trace.TableSessionQueryStreamExecuteDoneInfo) {
							if info.Error == nil {
								log.Debug("executed",
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
									zap.String("yql", query.String()),
									zap.String("params", params.String()),
									zap.Error(info.Error),
								)
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
						zap.String("version", version),
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
								zap.Error(info.Error),
							)
						}
						return func(info trace.TableSessionQueryStreamReadDoneInfo) {
							if info.Error == nil {
								log.Debug("read",
									zap.String("version", version),
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
		if details&trace.TableSessionTransactionEvents != 0 {
			log := log.Named("transaction")
			t.OnSessionTransactionBegin = func(info trace.TableSessionTransactionBeginStartInfo) func(trace.TableSessionTransactionBeginDoneInfo) {
				session := info.Session
				log.Debug("beginning",
					zap.String("version", version),
					zap.String("id", session.ID()),
					zap.String("status", session.Status()),
				)
				start := time.Now()
				return func(info trace.TableSessionTransactionBeginDoneInfo) {
					if info.Error == nil {
						log.Debug("began",
							zap.String("version", version),
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
					zap.String("version", version),
					zap.String("id", session.ID()),
					zap.String("status", session.Status()),
					zap.String("tx", tx.ID()),
				)
				start := time.Now()
				return func(info trace.TableSessionTransactionCommitDoneInfo) {
					if info.Error == nil {
						log.Debug("committed",
							zap.String("version", version),
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
					zap.String("version", version),
					zap.String("id", session.ID()),
					zap.String("status", session.Status()),
					zap.String("tx", tx.ID()),
				)
				start := time.Now()
				return func(info trace.TableSessionTransactionRollbackDoneInfo) {
					if info.Error == nil {
						log.Debug("rollback done",
							zap.String("version", version),
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
	if details&trace.TablePoolEvents != 0 {
		log := log.Named("pool")
		if details&trace.TablePoolSessionLifeCycleEvents != 0 {
			log := log.Named("session")
			t.OnPoolSessionNew = func(info trace.TablePoolSessionNewStartInfo) func(trace.TablePoolSessionNewDoneInfo) {
				log.Debug("try to create",
					zap.String("version", version),
				)
				start := time.Now()
				return func(info trace.TablePoolSessionNewDoneInfo) {
					if info.Error == nil {
						session := info.Session
						log.Debug("created",
							zap.String("version", version),
							zap.String("id", session.ID()),
							zap.String("status", session.Status()),
						)
					} else {
						log.Error("created",
							zap.String("version", version),
							zap.Duration("latency", time.Since(start)),
							zap.Error(info.Error),
						)
					}
				}
			}
			t.OnPoolSessionClose = func(info trace.TablePoolSessionCloseStartInfo) func(trace.TablePoolSessionCloseDoneInfo) {
				session := info.Session
				log.Debug("closing",
					zap.String("version", version),
					zap.String("id", session.ID()),
					zap.String("status", session.Status()),
				)
				start := time.Now()
				return func(info trace.TablePoolSessionCloseDoneInfo) {
					log.Debug("closed",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.String("id", session.ID()),
						zap.String("status", session.Status()),
					)
				}
			}
			t.OnPoolStateChange = func(info trace.TablePooStateChangeInfo) {
				log.Info("change",
					zap.String("version", version),
					zap.Int("size", info.Size),
					zap.String("event", info.Event),
				)
			}
		}
		if details&trace.TablePoolAPIEvents != 0 {
			t.OnPoolPut = func(info trace.TablePoolPutStartInfo) func(trace.TablePoolPutDoneInfo) {
				session := info.Session
				log.Debug("putting",
					zap.String("version", version),
					zap.String("id", session.ID()),
					zap.String("status", session.Status()),
				)
				start := time.Now()
				return func(info trace.TablePoolPutDoneInfo) {
					if info.Error == nil {
						log.Debug("put",
							zap.String("version", version),
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
				log.Debug("getting",
					zap.String("version", version),
				)
				start := time.Now()
				return func(info trace.TablePoolGetDoneInfo) {
					if info.Error == nil {
						session := info.Session
						log.Debug("got",
							zap.String("version", version),
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
				log.Debug("waiting",
					zap.String("version", version),
				)
				start := time.Now()
				return func(info trace.TablePoolWaitDoneInfo) {
					if info.Error == nil {
						if info.Session == nil {
							log.Debug(`wait done without any significant result`,
								zap.String("version", version),
								zap.Duration("latency", time.Since(start)),
							)
						} else {
							log.Debug(`wait done`,
								zap.String("version", version),
								zap.Duration("latency", time.Since(start)),
								zap.String("id", info.Session.ID()),
								zap.String("status", info.Session.Status()),
							)
						}
					} else {
						if info.Session == nil {
							log.Debug(`wait failed without any significant result`,
								zap.String("version", version),
								zap.Duration("latency", time.Since(start)),
								zap.Error(info.Error),
							)
						} else {
							log.Debug(`wait failed`,
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
