module github.com/ydb-platform/ydb-go-sdk-zap

go 1.16

require (
	github.com/ydb-platform/ydb-go-sdk/v3 v3.0.1-beta
	go.uber.org/zap v1.18.1
)

replace github.com/ydb-platform/ydb-go-sdk/v3 => ../ydb-go-sdk-private
