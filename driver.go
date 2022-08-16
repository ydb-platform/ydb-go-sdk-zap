package zap

import (
	"time"

	"go.uber.org/zap"

	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Driver makes trace.Driver with zap lging
func Driver(l *zap.Logger, details trace.Details, opts ...option) trace.Driver {
	l = l.Named("ydb").Named("driver")
	t := trace.Driver{}
	if details&trace.DriverNetEvents != 0 {
		l := l.Named("net")
		t.OnNetRead = func(info trace.DriverNetReadStartInfo) func(trace.DriverNetReadDoneInfo) {
			address := info.Address
			l.Debug("try to read",
				zap.String("address", address),
			)
			start := time.Now()
			return func(info trace.DriverNetReadDoneInfo) {
				if info.Error == nil {
					l.Debug("read",
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
				zap.String("address", address),
			)
			start := time.Now()
			return func(info trace.DriverNetWriteDoneInfo) {
				if info.Error == nil {
					l.Debug("wrote",
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
				zap.String("address", address),
			)
			start := time.Now()
			return func(info trace.DriverNetDialDoneInfo) {
				if info.Error == nil {
					l.Debug("dialed",
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
				zap.String("address", address),
			)
			start := time.Now()
			return func(info trace.DriverNetCloseDoneInfo) {
				if info.Error == nil {
					l.Debug("closed",
						zap.Duration("latency", time.Since(start)),
						zap.String("address", address),
					)
				} else {
					l.Warn("close failed",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.String("address", address),
						zap.Error(info.Error),
					)
				}
			}
		}
	}
	if details&trace.DriverRepeaterEvents != 0 {
		l := l.Named("repeater")
		t.OnRepeaterWakeUp = func(info trace.DriverRepeaterWakeUpStartInfo) func(trace.DriverRepeaterWakeUpDoneInfo) {
			name := info.Name
			event := info.Event
			l.Info("repeater wake up",
				zap.String("name", name),
				zap.String("event", event),
			)
			start := time.Now()
			return func(info trace.DriverRepeaterWakeUpDoneInfo) {
				if info.Error == nil {
					l.Info("repeater wake up done",
						zap.Duration("latency", time.Since(start)),
						zap.String("name", name),
						zap.String("event", event),
					)
				} else {
					l.Error("repeater wake up fail",
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
				zap.String("address", endpoint.Address()),
				zap.Time("lastUpdated", endpoint.LastUpdated()),
				zap.String("location", endpoint.Location()),
				zap.Bool("dataCenter", endpoint.LocalDC()),
			)
			start := time.Now()
			return func(info trace.DriverConnTakeDoneInfo) {
				if info.Error == nil {
					l.Debug("conn took",
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
		t.OnConnStateChange = func(info trace.DriverConnStateChangeStartInfo) func(trace.DriverConnStateChangeDoneInfo) {
			endpoint := info.Endpoint
			l.Debug("conn state change",
				zap.String("address", endpoint.Address()),
				zap.Time("lastUpdated", endpoint.LastUpdated()),
				zap.String("location", endpoint.Location()),
				zap.Bool("dataCenter", endpoint.LocalDC()),
				zap.String("state before", info.State.String()),
			)
			start := time.Now()
			return func(info trace.DriverConnStateChangeDoneInfo) {
				l.Info("conn state changed",
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
				zap.String("address", endpoint.Address()),
				zap.Time("lastUpdated", endpoint.LastUpdated()),
				zap.String("location", endpoint.Location()),
				zap.Bool("dataCenter", endpoint.LocalDC()),
			)
			start := time.Now()
			return func(info trace.DriverConnParkDoneInfo) {
				if info.Error == nil {
					l.Debug("parked",
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
				zap.String("address", endpoint.Address()),
				zap.Time("lastUpdated", endpoint.LastUpdated()),
				zap.String("location", endpoint.Location()),
				zap.Bool("dataCenter", endpoint.LocalDC()),
			)
			start := time.Now()
			return func(info trace.DriverConnCloseDoneInfo) {
				if info.Error == nil {
					l.Debug("closed",
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
		t.OnConnBan = func(info trace.DriverConnBanStartInfo) func(trace.DriverConnBanDoneInfo) {
			endpoint := info.Endpoint
			l.Debug("ban start",
				zap.String("address", endpoint.Address()),
				zap.Time("lastUpdated", endpoint.LastUpdated()),
				zap.String("location", endpoint.Location()),
				zap.Bool("localDC", endpoint.LocalDC()),
				zap.NamedError("cause", info.Cause),
			)
			start := time.Now()
			return func(info trace.DriverConnBanDoneInfo) {
				l.Warn("ban done",
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
		t.OnConnAllow = func(info trace.DriverConnAllowStartInfo) func(doneInfo trace.DriverConnAllowDoneInfo) {
			endpoint := info.Endpoint
			l.Debug("allow start",
				zap.String("address", endpoint.Address()),
				zap.Time("lastUpdated", endpoint.LastUpdated()),
				zap.String("location", endpoint.Location()),
				zap.Bool("localDC", endpoint.LocalDC()),
			)
			start := time.Now()
			return func(info trace.DriverConnAllowDoneInfo) {
				l.Debug("allow done",
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
	if details&trace.DriverBalancerEvents != 0 {
		l := l.Named("balancer")
		t.OnBalancerInit = func(info trace.DriverBalancerInitStartInfo) func(trace.DriverBalancerInitDoneInfo) {
			l.Debug("init start")
			start := time.Now()
			return func(info trace.DriverBalancerInitDoneInfo) {
				if info.Error == nil {
					l.Info("init done",
						zap.Duration("latency", time.Since(start)),
					)
				} else {
					l.Info("init failed",
						zap.Duration("latency", time.Since(start)),
						zap.Error(info.Error),
					)
				}
			}
		}
		t.OnBalancerClose = func(info trace.DriverBalancerCloseStartInfo) func(trace.DriverBalancerCloseDoneInfo) {
			l.Debug("close start")
			start := time.Now()
			return func(info trace.DriverBalancerCloseDoneInfo) {
				if info.Error == nil {
					l.Debug("close done",
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
		t.OnBalancerChooseEndpoint = func(info trace.DriverBalancerChooseEndpointStartInfo) func(doneInfo trace.DriverBalancerChooseEndpointDoneInfo) {
			l.Debug("try to choose endpoint")
			start := time.Now()
			return func(info trace.DriverBalancerChooseEndpointDoneInfo) {
				if info.Error == nil {
					l.Debug("endpoint choose ok",
						zap.Duration("latency", time.Since(start)),
						zap.String("address", info.Endpoint.Address()),
						zap.Bool("local", info.Endpoint.LocalDC()),
					)
				} else {
					l.Warn("endpoint choose failed",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.Error(info.Error),
					)
				}
			}
		}
		t.OnBalancerUpdate = func(info trace.DriverBalancerUpdateStartInfo) func(trace.DriverBalancerUpdateDoneInfo) {
			l.Debug("try to update balancer",
				zap.Bool("needLocalDC", info.NeedLocalDC),
			)
			start := time.Now()
			return func(info trace.DriverBalancerUpdateDoneInfo) {
				if info.Error == nil {
					endpoints := make([]string, 0, len(info.Endpoints))
					for _, e := range info.Endpoints {
						endpoints = append(endpoints, e.String())
					}
					l.Debug("endpoint choose ok",
						zap.Duration("latency", time.Since(start)),
						zap.Strings("endpoints", endpoints),
						zap.String("local", info.LocalDC),
					)
				} else {
					l.Warn("endpoint choose failed",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.Error(info.Error),
					)
				}
			}
		}
	}
	if details&trace.DriverCredentialsEvents != 0 {
		l := l.Named("credentials")
		t.OnGetCredentials = func(info trace.DriverGetCredentialsStartInfo) func(trace.DriverGetCredentialsDoneInfo) {
			l.Debug("getting")
			start := time.Now()
			return func(info trace.DriverGetCredentialsDoneInfo) {
				if info.Error == nil {
					l.Debug("got",
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
