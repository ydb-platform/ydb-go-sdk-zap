package zap

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"go.uber.org/zap"
	"time"
)

// Driver makes trace.Driver with zap logging
func Driver(log *zap.Logger, details trace.Details) trace.Driver {
	log = log.Named("ydb").Named("driver")
	t := trace.Driver{}
	if details&trace.DriverNetEvents != 0 {
		log := log.Named("net")
		t.OnNetRead = func(info trace.NetReadStartInfo) func(trace.NetReadDoneInfo) {
			address := info.Address
			log.Debug("try to read",
				zap.String("version", version),
				zap.String("address", address),
			)
			start := time.Now()
			return func(info trace.NetReadDoneInfo) {
				if info.Error == nil {
					log.Debug("read",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.String("address", address),
						zap.Int("received", info.Received),
					)
				} else {
					log.Warn("read failed",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.String("address", address),
						zap.Int("received", info.Received),
						zap.Error(info.Error),
					)
				}
			}
		}
		t.OnNetWrite = func(info trace.NetWriteStartInfo) func(trace.NetWriteDoneInfo) {
			address := info.Address
			log.Debug("try to write",
				zap.String("version", version),
				zap.String("address", address),
			)
			start := time.Now()
			return func(info trace.NetWriteDoneInfo) {
				if info.Error == nil {
					log.Debug("wrote",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.String("address", address),
						zap.Int("sent", info.Sent),
					)
				} else {
					log.Warn("write failed",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.String("address", address),
						zap.Int("sent", info.Sent),
						zap.Error(info.Error),
					)
				}
			}
		}
		t.OnNetDial = func(info trace.NetDialStartInfo) func(trace.NetDialDoneInfo) {
			address := info.Address
			log.Debug("try to dial",
				zap.String("version", version),
				zap.String("address", address),
			)
			start := time.Now()
			return func(info trace.NetDialDoneInfo) {
				if info.Error == nil {
					log.Debug("dialed",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.String("address", address),
					)
				} else {
					log.Error("dial failed",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.String("address", address),
						zap.Error(info.Error),
					)
				}
			}
		}
		t.OnNetClose = func(info trace.NetCloseStartInfo) func(trace.NetCloseDoneInfo) {
			address := info.Address
			log.Debug("try to close",
				zap.String("version", version),
				zap.String("address", address),
			)
			start := time.Now()
			return func(info trace.NetCloseDoneInfo) {
				if info.Error == nil {
					log.Debug("closed",
						zap.Duration("latency", time.Since(start)),
						zap.String("version", version),
						zap.String("address", address),
					)
				} else {
					log.Warn("close failed",
						zap.Duration("latency", time.Since(start)),
						zap.String("version", version),
						zap.String("address", address),
						zap.Error(info.Error),
					)
				}
			}
		}
	}
	if details&trace.DriverCoreEvents != 0 {
		log := log.Named("core")
		t.OnConnTake = func(info trace.ConnTakeStartInfo) func(trace.ConnTakeDoneInfo) {
			address := info.Endpoint.Address()
			dataCenter := info.Endpoint.LocalDC()
			log.Debug("try to take conn",
				zap.String("version", version),
				zap.String("address", address),
				zap.Bool("dataCenter", dataCenter),
			)
			start := time.Now()
			return func(info trace.ConnTakeDoneInfo) {
				if info.Error == nil {
					log.Debug("conn took",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.String("address", address),
						zap.Bool("dataCenter", dataCenter),
					)
				} else {
					log.Warn("conn take failed",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.String("address", address),
						zap.Bool("dataCenter", dataCenter),
						zap.Error(info.Error),
					)
				}
			}
		}
		t.OnConnRelease = func(info trace.ConnReleaseStartInfo) func(trace.ConnReleaseDoneInfo) {
			address := info.Endpoint.Address()
			dataCenter := info.Endpoint.LocalDC()
			log.Debug("try to release conn",
				zap.String("version", version),
				zap.String("address", address),
				zap.Bool("dataCenter", dataCenter),
			)
			start := time.Now()
			return func(info trace.ConnReleaseDoneInfo) {
				log.Debug("conn released",
					zap.String("version", version),
					zap.Duration("latency", time.Since(start)),
					zap.String("address", address),
					zap.Bool("dataCenter", dataCenter),
					zap.Int("locks", info.Lock),
				)
			}
		}
		t.OnConnStateChange = func(info trace.ConnStateChangeStartInfo) func(trace.ConnStateChangeDoneInfo) {
			address := info.Endpoint.Address()
			dataCenter := info.Endpoint.LocalDC()
			log.Debug("conn state change",
				zap.String("version", version),
				zap.String("address", address),
				zap.Bool("dataCenter", dataCenter),
				zap.String("state before", info.State.String()),
			)
			start := time.Now()
			return func(info trace.ConnStateChangeDoneInfo) {
				log.Debug("conn state changed",
					zap.String("version", version),
					zap.Duration("latency", time.Since(start)),
					zap.String("address", address),
					zap.Bool("dataCenter", dataCenter),
					zap.String("state after", info.State.String()),
				)
			}
		}
		t.OnConnInvoke = func(info trace.ConnInvokeStartInfo) func(trace.ConnInvokeDoneInfo) {
			address := info.Endpoint.Address()
			dataCenter := info.Endpoint.LocalDC()
			method := string(info.Method)
			log.Debug("try to invoke",
				zap.String("version", version),
				zap.String("address", address),
				zap.Bool("dataCenter", dataCenter),
				zap.String("method", method),
			)
			start := time.Now()
			return func(info trace.ConnInvokeDoneInfo) {
				if info.Error == nil {
					log.Debug("invoked",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.String("address", address),
						zap.Bool("dataCenter", dataCenter),
						zap.String("method", method),
					)
				} else {
					log.Warn("invoke failed",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.String("address", address),
						zap.Bool("dataCenter", dataCenter),
						zap.String("method", method),
						zap.Error(info.Error),
					)
				}
			}
		}
		t.OnConnNewStream = func(info trace.ConnNewStreamStartInfo) func(trace.ConnNewStreamRecvInfo) func(trace.ConnNewStreamDoneInfo) {
			address := info.Endpoint.Address()
			dataCenter := info.Endpoint.LocalDC()
			method := string(info.Method)
			log.Debug("try to streaming",
				zap.String("version", version),
				zap.String("address", address),
				zap.Bool("dataCenter", dataCenter),
				zap.String("method", method),
			)
			start := time.Now()
			return func(info trace.ConnNewStreamRecvInfo) func(trace.ConnNewStreamDoneInfo) {
				if info.Error == nil {
					log.Debug("streaming intermediate receive",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.String("address", address),
						zap.Bool("dataCenter", dataCenter),
						zap.String("method", method),
					)
				} else {
					log.Warn("streaming intermediate receive failed",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.String("address", address),
						zap.Bool("dataCenter", dataCenter),
						zap.String("method", method),
						zap.Error(info.Error),
					)
				}
				return func(info trace.ConnNewStreamDoneInfo) {
					if info.Error == nil {
						log.Debug("streaming finished",
							zap.String("version", version),
							zap.Duration("latency", time.Since(start)),
							zap.String("address", address),
							zap.Bool("dataCenter", dataCenter),
							zap.String("method", method),
						)
					} else {
						log.Warn("streaming failed",
							zap.String("version", version),
							zap.Duration("latency", time.Since(start)),
							zap.String("address", address),
							zap.Bool("dataCenter", dataCenter),
							zap.String("method", method),
							zap.Error(info.Error),
						)
					}
				}
			}
		}
	}
	if details&trace.DriverDiscoveryEvents != 0 {
		log := log.Named("discovery")
		t.OnDiscovery = func(info trace.DiscoveryStartInfo) func(trace.DiscoveryDoneInfo) {
			log.Debug("try to discover",
				zap.String("version", version),
			)
			start := time.Now()
			return func(info trace.DiscoveryDoneInfo) {
				if info.Error == nil {
					log.Debug("discover finished",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.Strings("endpoints", info.Endpoints),
					)
				} else {
					log.Error("discover failed",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.Error(info.Error),
					)
				}
			}
		}
	}
	if details&trace.DriverClusterEvents != 0 {
		log := log.Named("cluster")
		t.OnClusterGet = func(info trace.ClusterGetStartInfo) func(trace.ClusterGetDoneInfo) {
			log.Debug("try to get conn",
				zap.String("version", version),
			)
			start := time.Now()
			return func(info trace.ClusterGetDoneInfo) {
				if info.Error == nil {
					log.Debug("conn got",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.String("address", info.Endpoint.Address()),
						zap.Bool("local", info.Endpoint.LocalDC()),
					)
				} else {
					log.Warn("conn get failed",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.Error(info.Error),
					)
				}
			}
		}
		t.OnClusterInsert = func(info trace.ClusterInsertStartInfo) func(trace.ClusterInsertDoneInfo) {
			address := info.Endpoint.Address()
			dataCenter := info.Endpoint.LocalDC()
			log.Debug("inserting",
				zap.String("version", version),
				zap.String("address", address),
				zap.Bool("local", dataCenter),
			)
			start := time.Now()
			return func(info trace.ClusterInsertDoneInfo) {
				log.Info("inserted",
					zap.String("version", version),
					zap.Duration("latency", time.Since(start)),
					zap.String("address", address),
					zap.Bool("local", dataCenter),
					zap.String("state", info.State.String()),
				)
			}
		}
		t.OnClusterRemove = func(info trace.ClusterRemoveStartInfo) func(trace.ClusterRemoveDoneInfo) {
			address := info.Endpoint.Address()
			dataCenter := info.Endpoint.LocalDC()
			log.Debug("removing",
				zap.String("version", version),
				zap.String("address", address),
				zap.Bool("local", dataCenter),
			)
			start := time.Now()
			return func(info trace.ClusterRemoveDoneInfo) {
				log.Info("removed",
					zap.String("version", version),
					zap.Duration("latency", time.Since(start)),
					zap.String("address", address),
					zap.Bool("local", dataCenter),
					zap.String("state", info.State.String()),
				)
			}
		}
		t.OnClusterUpdate = func(info trace.ClusterUpdateStartInfo) func(trace.ClusterUpdateDoneInfo) {
			address := info.Endpoint.Address()
			dataCenter := info.Endpoint.LocalDC()
			log.Debug("updating",
				zap.String("version", version),
				zap.String("address", address),
				zap.Bool("local", dataCenter),
			)
			start := time.Now()
			return func(info trace.ClusterUpdateDoneInfo) {
				log.Info("updated",
					zap.String("version", version),
					zap.Duration("latency", time.Since(start)),
					zap.String("address", address),
					zap.Bool("local", dataCenter),
					zap.String("state", info.State.String()),
				)
			}
		}
		t.OnPessimizeNode = func(info trace.PessimizeNodeStartInfo) func(trace.PessimizeNodeDoneInfo) {
			address := info.Endpoint.Address()
			dataCenter := info.Endpoint.LocalDC()
			log.Warn("pessimizing",
				zap.String("version", version),
				zap.String("address", address),
				zap.Bool("local", dataCenter),
				zap.NamedError("cause", info.Cause),
			)
			start := time.Now()
			return func(info trace.PessimizeNodeDoneInfo) {
				log.Warn("pessimized",
					zap.String("version", version),
					zap.Duration("latency", time.Since(start)),
					zap.String("address", address),
					zap.Bool("local", dataCenter),
					zap.String("state", info.State.String()),
					zap.Error(info.Error),
				)
			}
		}
	}
	if details&trace.DriverCredentialsEvents != 0 {
		log := log.Named("credentials")
		t.OnGetCredentials = func(info trace.GetCredentialsStartInfo) func(trace.GetCredentialsDoneInfo) {
			log.Debug("getting",
				zap.String("version", version),
			)
			start := time.Now()
			return func(info trace.GetCredentialsDoneInfo) {
				if info.Error == nil {
					log.Debug("got",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.Bool("token ok", info.TokenOk),
					)
				} else {
					log.Error("get failed",
						zap.String("version", version),
						zap.Duration("latency", time.Since(start)),
						zap.Bool("token ok", info.TokenOk),
						zap.Error(info.Error),
					)
				}
			}
		}
	}
	return t
}
