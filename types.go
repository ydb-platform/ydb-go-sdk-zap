package zap

import (
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"path"
)

var (
	version = func() string {
		_, version := path.Split(ydb.Version)
		return version
	}()
)
