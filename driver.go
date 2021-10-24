package zap

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"go.uber.org/zap"
	"time"
)

// Driver makes Driver with zap logging
func Driver(log *zap.Logger, details Details) trace.Driver {
	log = log.Named("ydb").Named("driver")
	t := trace.Driver{}
	if details&driverNetEvents != 0 {
		log := log.Named("net")
		t.OnNetRead = func(info trace.NetReadStartInfo) func(trace.NetReadDoneInfo) {
			address := info.Address
			log.Debug("try to read",
				zap.String("version", version),
				zap.String("address", address),
			)
			start := time.Now()
			return func(info trace.NetReadDoneInfo) {
				log.Debug("read",
					zap.String("version", version),
					zap.Duration("latency", time.Since(start)),
					zap.String("address", address),
					zap.Int("received", info.Received),
					zap.Error(info.Error),
				)
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
				log.Debug("wrote",
					zap.String("version", version),
					zap.Duration("latency", time.Since(start)),
					zap.String("address", address),
					zap.Int("sent", info.Sent),
					zap.Error(info.Error),
				)
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
				log.Debug("dialed",
					zap.String("version", version),
					zap.Duration("latency", time.Since(start)),
					zap.String("address", address),
					zap.Error(info.Error),
				)
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
				log.Debug("closed",
					zap.Duration("latency", time.Since(start)),
					zap.String("version", version),
					zap.String("address", address),
					zap.Error(info.Error),
				)
			}
		}
	}
	if details&DriverCoreEvents != 0 {
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
				log.Debug("conn took",
					zap.String("version", version),
					zap.Duration("latency", time.Since(start)),
					zap.String("address", address),
					zap.Bool("dataCenter", dataCenter),
					zap.Error(info.Error),
				)
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
				log.Debug("invoked",
					zap.String("version", version),
					zap.Duration("latency", time.Since(start)),
					zap.String("address", address),
					zap.Bool("dataCenter", dataCenter),
					zap.String("method", method),
					zap.Error(info.Error),
				)
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
				log.Debug("streaming intermediate receive",
					zap.String("version", version),
					zap.Duration("latency", time.Since(start)),
					zap.String("address", address),
					zap.Bool("dataCenter", dataCenter),
					zap.String("method", method),
					zap.Error(info.Error),
				)
				return func(info trace.ConnNewStreamDoneInfo) {
					log.Debug("streaming finished",
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
	if details&DriverDiscoveryEvents != 0 {
		log := log.Named("discovery")
		t.OnDiscovery = func(info trace.DiscoveryStartInfo) func(trace.DiscoveryDoneInfo) {
			log.Debug("try to discover",
				zap.String("version", version),
			)
			start := time.Now()
			return func(info trace.DiscoveryDoneInfo) {
				log.Debug("discover finished",
					zap.String("version", version),
					zap.Duration("latency", time.Since(start)),
					zap.Strings("endpoints", info.Endpoints),
					zap.Error(info.Error),
				)
			}
		}
	}
	if details&DriverClusterEvents != 0 {
		log := log.Named("cluster")
		t.OnClusterGet = func(info trace.ClusterGetStartInfo) func(trace.ClusterGetDoneInfo) {
			log.Debug("try to get conn",
				zap.String("version", version),
			)
			start := time.Now()
			return func(info trace.ClusterGetDoneInfo) {
				log.Debug("conn got",
					zap.String("version", version),
					zap.Duration("latency", time.Since(start)),
					zap.String("address", info.Endpoint.Address()),
					zap.Bool("local", info.Endpoint.LocalDC()),
					zap.Error(info.Error),
				)
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
				log.Debug("inserted",
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
				log.Debug("removed",
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
				log.Debug("updated",
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
			log.Debug("pessimizing",
				zap.String("version", version),
				zap.String("address", address),
				zap.Bool("local", dataCenter),
			)
			start := time.Now()
			return func(info trace.PessimizeNodeDoneInfo) {
				log.Debug("pessimized",
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
	if details&DriverCredentialsEvents != 0 {
		log := log.Named("credentials")
		t.OnGetCredentials = func(info trace.GetCredentialsStartInfo) func(trace.GetCredentialsDoneInfo) {
			log.Debug("getting",
				zap.String("version", version),
			)
			start := time.Now()
			return func(info trace.GetCredentialsDoneInfo) {
				log.Debug("got",
					zap.String("version", version),
					zap.Duration("latency", time.Since(start)),
					zap.Bool("token ok", info.TokenOk),
					zap.Error(info.Error),
				)
			}
		}
	}
	return t
}
