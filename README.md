# metrics

metrics package helps to create ydb-go-sdk traces with monitoring internal state of driver

## Usage
```go
import (
    "fmt"
    "sync/mutex"
    "time"

    "go.uber.org/zap"

    "github.com/ydb-platform/ydb-go-sdk/v3"

    ydbZap "github.com/ydb-platform/ydb-go-sdk-zap"
)

func main() {
	// init your zap.Logger
	log, err := zap.NewProduction()
	
    db, err := ydb.New(
        context.Background(),
		ydb.MustConnectionString(connection),
		ydb.WithTraceDriver(ydbZap.Driver(
			log,
			ydbZap.DetailsAll,
		)),
		ydb.WithTraceTable(ydbZap.Table(
			log,
			ydbZap.DetailsAll,
		)),
	)
    // work with db
}
```
