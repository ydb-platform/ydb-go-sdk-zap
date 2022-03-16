package zap

import (
	"time"

	"go.uber.org/zap"

	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Driver makes trace.Driver with zap lging
func Driver(l *zap.Logger, details trace.Details) trace.Driver {
	l = l.Named("ydb").Named("driver")
	t := trace.Driver{}
	if details&trace.DriverNetEvents != 0 {
		l := l.Named("net")
		t.OnNetRead = func(info trace.DriverNetReadStartInfo) func(trace.DriverNetReadDoneInfo) {
			address := info.Address
			l.Debug("try to read",
				zap.String("version", version),
				zap.String("address", address),
			)
			start := time.Now()
			return func(info trace.DriverNetReadDoneInfo) {
				if info.Error == nil {
					l.Debug("read",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.String("address", address),
						zap.Int("received", info.Received),
					)
				} else {
					l.Warn("read failed",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.String("address", address),
						zap.Int("received", info.Received),
						zap.Error(info.Error),
					)
				}
			}
		}
		t.OnNetWrite = func(info trace.DriverNetWriteStartInfo) func(trace.DriverNetWriteDoneInfo) {
			address := info.Address
			l.Debug("try to write",
				zap.String("version", version),
				zap.String("address", address),
			)
			start := time.Now()
			return func(info trace.DriverNetWriteDoneInfo) {
				if info.Error == nil {
					l.Debug("wrote",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.String("address", address),
						zap.Int("sent", info.Sent),
					)
				} else {
					l.Warn("write failed",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.String("address", address),
						zap.Int("sent", info.Sent),
						zap.Error(info.Error),
					)
				}
			}
		}
		t.OnNetDial = func(info trace.DriverNetDialStartInfo) func(trace.DriverNetDialDoneInfo) {
			address := info.Address
			l.Debug("try to dial",
				zap.String("version", version),
				zap.String("address", address),
			)
			start := time.Now()
			return func(info trace.DriverNetDialDoneInfo) {
				if info.Error == nil {
					l.Debug("dialed",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.String("address", address),
					)
				} else {
					l.Warn("dial failed",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.String("address", address),
						zap.Error(info.Error),
					)
				}
			}
		}
		t.OnNetClose = func(info trace.DriverNetCloseStartInfo) func(trace.DriverNetCloseDoneInfo) {
			address := info.Address
			l.Debug("try to close",
				zap.String("version", version),
				zap.String("address", address),
			)
			start := time.Now()
			return func(info trace.DriverNetCloseDoneInfo) {
				if info.Error == nil {
					l.Debug("closed",
						zap.Duration("latency", time.Since(start)),
						zap.String("version", version),
						zap.String("address", address),
					)
				} else {
					l.Warn("close failed",
						zap.Duration("latency", time.Since(start)),
						zap.String("version", version),
						zap.String("address", address),
						zap.Error(info.Error),
					)
				}
			}
		}
	}
	if details&trace.DriverRepeaterEvents != 0 {
		l := l.Named("repeater")
		t.OnRepeaterWakeUp = func(info trace.DriverRepeaterTickStartInfo) func(trace.DriverRepeaterTickDoneInfo) {
			name := info.Name
			event := info.Event
			l.Info("repeater wake up",
				zap.String("version", version),
				zap.String("name", name),
				zap.String("event", event),
			)
			start := time.Now()
			return func(info trace.DriverRepeaterTickDoneInfo) {
				if info.Error == nil {
					l.Info("repeater wake up done",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.String("name", name),
						zap.String("event", event),
					)
				} else {
					l.Info("repeater wake up fail",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.String("name", name),
						zap.String("event", event),
						zap.Error(info.Error),
					)
				}
			}
		}
	}
	if details&trace.DriverConnEvents != 0 {
		l := l.Named("conn")
		t.OnConnTake = func(info trace.DriverConnTakeStartInfo) func(trace.DriverConnTakeDoneInfo) {
			endpoint := info.Endpoint
			l.Debug("try to take conn",
				zap.String("version", version),
				zap.String("address", endpoint.Address()),
				zap.Time("lastUpdated", endpoint.LastUpdated()),
				zap.String("location", endpoint.Location()),
				zap.Bool("dataCenter", endpoint.LocalDC()),
			)
			start := time.Now()
			return func(info trace.DriverConnTakeDoneInfo) {
				if info.Error == nil {
					l.Debug("conn took",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.String("address", endpoint.Address()),
						zap.Time("lastUpdated", endpoint.LastUpdated()),
						zap.String("location", endpoint.Location()),
						zap.Bool("dataCenter", endpoint.LocalDC()),
					)
				} else {
					l.Warn("conn take failed",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.String("address", endpoint.Address()),
						zap.Time("lastUpdated", endpoint.LastUpdated()),
						zap.String("location", endpoint.Location()),
						zap.Bool("dataCenter", endpoint.LocalDC()),
						zap.Error(info.Error),
					)
				}
			}
		}
		t.OnConnUsagesChange = func(info trace.DriverConnUsagesChangeInfo) {
			l.Debug("conn usages changed",
				zap.String("version", version),
				zap.String("address", info.Endpoint.Address()),
				zap.Time("lastUpdated", info.Endpoint.LastUpdated()),
				zap.String("location", info.Endpoint.Location()),
				zap.Bool("dataCenter", info.Endpoint.LocalDC()),
				zap.Int("usages", info.Usages),
			)

		}
		t.OnConnStateChange = func(info trace.DriverConnStateChangeStartInfo) func(trace.DriverConnStateChangeDoneInfo) {
			endpoint := info.Endpoint
			l.Debug("conn state change",
				zap.String("version", version),
				zap.String("address", endpoint.Address()),
				zap.Time("lastUpdated", endpoint.LastUpdated()),
				zap.String("location", endpoint.Location()),
				zap.Bool("dataCenter", endpoint.LocalDC()),
				zap.String("state before", info.State.String()),
			)
			start := time.Now()
			return func(info trace.DriverConnStateChangeDoneInfo) {
				l.Debug("conn state changed",
					zap.String("version", version),
					zap.Duration("latency", time.Since(start)),
					zap.String("address", endpoint.Address()),
					zap.Time("lastUpdated", endpoint.LastUpdated()),
					zap.String("location", endpoint.Location()),
					zap.Bool("dataCenter", endpoint.LocalDC()),
					zap.String("state after", info.State.String()),
				)
			}
		}
		t.OnConnInvoke = func(info trace.DriverConnInvokeStartInfo) func(trace.DriverConnInvokeDoneInfo) {
			endpoint := info.Endpoint
			method := string(info.Method)
			l.Debug("try to invoke",
				zap.String("version", version),
				zap.String("address", endpoint.Address()),
				zap.Time("lastUpdated", endpoint.LastUpdated()),
				zap.String("location", endpoint.Location()),
				zap.Bool("dataCenter", endpoint.LocalDC()),
				zap.String("method", method),
			)
			start := time.Now()
			return func(info trace.DriverConnInvokeDoneInfo) {
				if info.Error == nil {
					l.Debug("invoked",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.String("address", endpoint.Address()),
						zap.Time("lastUpdated", endpoint.LastUpdated()),
						zap.String("location", endpoint.Location()),
						zap.Bool("dataCenter", endpoint.LocalDC()),
						zap.String("method", method),
					)
				} else {
					l.Warn("invoke failed",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.String("address", endpoint.Address()),
						zap.Time("lastUpdated", endpoint.LastUpdated()),
						zap.String("location", endpoint.Location()),
						zap.Bool("dataCenter", endpoint.LocalDC()),
						zap.String("method", method),
						zap.Error(info.Error),
					)
				}
			}
		}
		t.OnConnNewStream = func(info trace.DriverConnNewStreamStartInfo) func(trace.DriverConnNewStreamRecvInfo) func(trace.DriverConnNewStreamDoneInfo) {
			endpoint := info.Endpoint
			method := string(info.Method)
			l.Debug("try to streaming",
				zap.String("version", version),
				zap.String("address", endpoint.Address()),
				zap.Time("lastUpdated", endpoint.LastUpdated()),
				zap.String("location", endpoint.Location()),
				zap.Bool("dataCenter", endpoint.LocalDC()),
				zap.String("method", method),
			)
			start := time.Now()
			return func(info trace.DriverConnNewStreamRecvInfo) func(trace.DriverConnNewStreamDoneInfo) {
				if info.Error == nil {
					l.Debug("streaming intermediate receive",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.String("address", endpoint.Address()),
						zap.Time("lastUpdated", endpoint.LastUpdated()),
						zap.String("location", endpoint.Location()),
						zap.Bool("dataCenter", endpoint.LocalDC()),
						zap.String("method", method),
					)
				} else {
					l.Warn("streaming intermediate receive failed",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.String("address", endpoint.Address()),
						zap.Time("lastUpdated", endpoint.LastUpdated()),
						zap.String("location", endpoint.Location()),
						zap.Bool("dataCenter", endpoint.LocalDC()),
						zap.String("method", method),
						zap.Error(info.Error),
					)
				}
				return func(info trace.DriverConnNewStreamDoneInfo) {
					if info.Error == nil {
						l.Debug("streaming finished",
							zap.String("version", version),
							zap.Duration("latency", time.Since(start)),
							zap.String("address", endpoint.Address()),
							zap.Time("lastUpdated", endpoint.LastUpdated()),
							zap.String("location", endpoint.Location()),
							zap.Bool("dataCenter", endpoint.LocalDC()),
							zap.String("method", method),
						)
					} else {
						l.Warn("streaming failed",
							zap.String("version", version),
							zap.Duration("latency", time.Since(start)),
							zap.String("address", endpoint.Address()),
							zap.Time("lastUpdated", endpoint.LastUpdated()),
							zap.String("location", endpoint.Location()),
							zap.Bool("dataCenter", endpoint.LocalDC()),
							zap.String("method", method),
							zap.Error(info.Error),
						)
					}
				}
			}
		}
		t.OnConnPark = func(info trace.DriverConnParkStartInfo) func(trace.DriverConnParkDoneInfo) {
			endpoint := info.Endpoint
			l.Debug("try to park",
				zap.String("version", version),
				zap.String("address", endpoint.Address()),
				zap.Time("lastUpdated", endpoint.LastUpdated()),
				zap.String("location", endpoint.Location()),
				zap.Bool("dataCenter", endpoint.LocalDC()),
			)
			start := time.Now()
			return func(info trace.DriverConnParkDoneInfo) {
				if info.Error == nil {
					l.Debug("parked",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.String("address", endpoint.Address()),
						zap.Time("lastUpdated", endpoint.LastUpdated()),
						zap.String("location", endpoint.Location()),
						zap.Bool("dataCenter", endpoint.LocalDC()),
					)
				} else {
					l.Warn("park failed",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.String("address", endpoint.Address()),
						zap.Time("lastUpdated", endpoint.LastUpdated()),
						zap.String("location", endpoint.Location()),
						zap.Bool("dataCenter", endpoint.LocalDC()),
						zap.Error(info.Error),
					)
				}
			}
		}
		t.OnConnClose = func(info trace.DriverConnCloseStartInfo) func(trace.DriverConnCloseDoneInfo) {
			endpoint := info.Endpoint
			l.Debug("try to close",
				zap.String("version", version),
				zap.String("address", endpoint.Address()),
				zap.Time("lastUpdated", endpoint.LastUpdated()),
				zap.String("location", endpoint.Location()),
				zap.Bool("dataCenter", endpoint.LocalDC()),
			)
			start := time.Now()
			return func(info trace.DriverConnCloseDoneInfo) {
				if info.Error == nil {
					l.Debug("closed",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.String("address", endpoint.Address()),
						zap.Time("lastUpdated", endpoint.LastUpdated()),
						zap.String("location", endpoint.Location()),
						zap.Bool("dataCenter", endpoint.LocalDC()),
					)
				} else {
					l.Warn("close failed",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.String("address", endpoint.Address()),
						zap.Time("lastUpdated", endpoint.LastUpdated()),
						zap.String("location", endpoint.Location()),
						zap.Bool("dataCenter", endpoint.LocalDC()),
						zap.Error(info.Error),
					)
				}
			}
		}
	}
	if details&trace.DriverClusterEvents != 0 {
		l := l.Named("cluster")
		t.OnClusterInit = func(info trace.DriverClusterInitStartInfo) func(trace.DriverClusterInitDoneInfo) {
			l.Debug("init start",
				zap.String("version", version),
			)
			start := time.Now()
			return func(info trace.DriverClusterInitDoneInfo) {
				l.Info("init done",
					zap.String("version", version),
					zap.Duration("latency", time.Since(start)),
				)
			}
		}
		t.OnClusterClose = func(info trace.DriverClusterCloseStartInfo) func(trace.DriverClusterCloseDoneInfo) {
			l.Debug("close start",
				zap.String("version", version),
			)
			start := time.Now()
			return func(info trace.DriverClusterCloseDoneInfo) {
				if info.Error == nil {
					l.Debug("close done",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
					)
				} else {
					l.Warn("close failed",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.Error(info.Error),
					)
				}
			}
		}
		t.OnClusterGet = func(info trace.DriverClusterGetStartInfo) func(trace.DriverClusterGetDoneInfo) {
			l.Debug("try to get conn",
				zap.String("version", version),
			)
			start := time.Now()
			return func(info trace.DriverClusterGetDoneInfo) {
				if info.Error == nil {
					l.Debug("conn got",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.String("address", info.Endpoint.Address()),
						zap.Bool("local", info.Endpoint.LocalDC()),
					)
				} else {
					l.Warn("conn get failed",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.Error(info.Error),
					)
				}
			}
		}
		t.OnClusterInsert = func(info trace.DriverClusterInsertStartInfo) func(trace.DriverClusterInsertDoneInfo) {
			endpoint := info.Endpoint
			l.Debug("inserting",
				zap.String("version", version),
				zap.String("address", endpoint.Address()),
				zap.Time("lastUpdated", endpoint.LastUpdated()),
				zap.String("location", endpoint.Location()),
				zap.Bool("localDC", endpoint.LocalDC()),
			)
			start := time.Now()
			return func(info trace.DriverClusterInsertDoneInfo) {
				l.Info("inserted",
					zap.String("version", version),
					zap.Duration("latency", time.Since(start)),
					zap.String("address", endpoint.Address()),
					zap.Time("lastUpdated", endpoint.LastUpdated()),
					zap.String("location", endpoint.Location()),
					zap.Bool("localDC", endpoint.LocalDC()),
					zap.String("state", info.State.String()),
				)
			}
		}
		t.OnClusterRemove = func(info trace.DriverClusterRemoveStartInfo) func(trace.DriverClusterRemoveDoneInfo) {
			endpoint := info.Endpoint
			l.Debug("removing",
				zap.String("version", version),
				zap.String("address", endpoint.Address()),
				zap.Time("lastUpdated", endpoint.LastUpdated()),
				zap.String("location", endpoint.Location()),
				zap.Bool("localDC", endpoint.LocalDC()),
			)
			start := time.Now()
			return func(info trace.DriverClusterRemoveDoneInfo) {
				l.Info("removed",
					zap.String("version", version),
					zap.Duration("latency", time.Since(start)),
					zap.String("address", endpoint.Address()),
					zap.Time("lastUpdated", endpoint.LastUpdated()),
					zap.String("location", endpoint.Location()),
					zap.Bool("localDC", endpoint.LocalDC()),
					zap.String("state", info.State.String()),
				)
			}
		}
		t.OnClusterUpdate = func(info trace.DriverClusterUpdateStartInfo) func(trace.DriverClusterUpdateDoneInfo) {
			endpoint := info.Endpoint
			l.Debug("updating",
				zap.String("version", version),
				zap.String("address", endpoint.Address()),
				zap.Time("lastUpdated", endpoint.LastUpdated()),
				zap.String("location", endpoint.Location()),
				zap.Bool("localDC", endpoint.LocalDC()),
			)
			start := time.Now()
			return func(info trace.DriverClusterUpdateDoneInfo) {
				l.Info("updated",
					zap.String("version", version),
					zap.Duration("latency", time.Since(start)),
					zap.String("address", endpoint.Address()),
					zap.Time("lastUpdated", endpoint.LastUpdated()),
					zap.String("location", endpoint.Location()),
					zap.Bool("localDC", endpoint.LocalDC()),
					zap.String("state", info.State.String()),
				)
			}
		}
		t.OnPessimizeNode = func(info trace.DriverPessimizeNodeStartInfo) func(trace.DriverPessimizeNodeDoneInfo) {
			endpoint := info.Endpoint
			l.Warn("pessimizing",
				zap.String("version", version),
				zap.String("address", endpoint.Address()),
				zap.Time("lastUpdated", endpoint.LastUpdated()),
				zap.String("location", endpoint.Location()),
				zap.Bool("localDC", endpoint.LocalDC()),
				zap.NamedError("cause", info.Cause),
			)
			start := time.Now()
			return func(info trace.DriverPessimizeNodeDoneInfo) {
				l.Warn("pessimized",
					zap.String("version", version),
					zap.Duration("latency", time.Since(start)),
					zap.String("address", endpoint.Address()),
					zap.Time("lastUpdated", endpoint.LastUpdated()),
					zap.String("location", endpoint.Location()),
					zap.Bool("localDC", endpoint.LocalDC()),
					zap.String("state", info.State.String()),
				)
			}
		}
	}
	if details&trace.DriverCredentialsEvents != 0 {
		l := l.Named("credentials")
		t.OnGetCredentials = func(info trace.DriverGetCredentialsStartInfo) func(trace.DriverGetCredentialsDoneInfo) {
			l.Debug("getting",
				zap.String("version", version),
			)
			start := time.Now()
			return func(info trace.DriverGetCredentialsDoneInfo) {
				if info.Error == nil {
					l.Debug("got",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.String("token", log.Secret(info.Token)),
					)
				} else {
					l.Error("get failed",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.Error(info.Error),
					)
				}
			}
		}
	}
	return t
}
