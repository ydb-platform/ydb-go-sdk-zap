package zap

import (
	"path"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var (
	version = func() string {
		_, version := path.Split(ydb.Version)
		return version
	}()
)

type detailer interface {
	Details() trace.Details
}
