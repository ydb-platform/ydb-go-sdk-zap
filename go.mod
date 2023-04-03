module github.com/ydb-platform/ydb-go-sdk-zap

go 1.16

require (
	github.com/ydb-platform/ydb-go-sdk/v3 v3.42.5
	go.uber.org/zap v1.20.0
)

replace github.com/ydb-platform/ydb-go-sdk/v3 v3.42.5 => github.com/ydb-platform/ydb-go-sdk/v3 v3.42.11-0.20230215114940-412e3214d6f1
