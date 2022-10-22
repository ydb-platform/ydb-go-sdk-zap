# zap

zap package helps to create ydb-go-sdk traces with logging driver events with zap

## Usage
```go
import (
    "fmt"
    "sync/mutex"
    "time"

    "go.uber.org/zap"

    "github.com/ydb-platform/ydb-go-sdk/v3"
    "github.com/ydb-platform/ydb-go-sdk/v3/trace"

    ydbZap "github.com/ydb-platform/ydb-go-sdk-zap"
)

func main() {
    // init your zap.Logger
    log, err := zap.NewProduction()
	
    db, err := ydb.Open(context.Background(),
        os.Getenv("YDB_CONNECTION_STRING"),
        ydbZap.WithTraces(
            log,
            trace.DetailsAll,
        ),
    )
    // work with db
}
```
