package zap

import (
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"path"
)

type Details int

const (
	DriverClusterEvents = Details(1 << iota)
	driverNetEvents
	DriverCoreEvents
	DriverCredentialsEvents
	DriverDiscoveryEvents

	tableSessionEvents
	tableSessionQueryInvokeEvents
	tableSessionQueryStreamEvents
	tableSessionTransactionEvents
	tablePoolLifeCycleEvents
	tablePoolRetryEvents
	tablePoolSessionLifeCycleEvents
	tablePoolAPIEvents

	DriverConnEvents        = driverNetEvents | DriverCoreEvents
	tableSessionQueryEvents = tableSessionQueryInvokeEvents | tableSessionQueryStreamEvents
	TableSessionEvents      = tableSessionEvents | tableSessionQueryEvents | tableSessionTransactionEvents
	TablePoolEvents         = tablePoolLifeCycleEvents | tablePoolRetryEvents | tablePoolSessionLifeCycleEvents | tablePoolAPIEvents
	DetailsAll              = ^Details(0)
)

var (
	version = func() string {
		_, version := path.Split(ydb.Version)
		return version
	}()
)
