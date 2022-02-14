package main

import (
	"context"
	"fmt"
	"math/rand"
	_ "net/http/pprof"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"

	ydbZap "github.com/ydb-platform/ydb-go-sdk-zap"
)

var (
	log *zap.Logger
)

func init() {
	var err error
	log, err = zap.NewDevelopment(
		zap.IncreaseLevel(
			func() zapcore.Level {
				if logLevel, ok := os.LookupEnv("LOG_LEVEL"); ok {
					for l := zapcore.DebugLevel; l < zapcore.FatalLevel; l++ {
						if l.CapitalString() == strings.ToUpper(logLevel) {
							return l
						}
					}
				}
				return zapcore.DebugLevel
			}(),
		),
	)
	if err != nil {
		panic(err)
	}
}

func main() {
	ctx := context.Background()
	var creds ydb.Option
	if token, has := os.LookupEnv("YDB_ACCESS_TOKEN_CREDENTIALS"); has {
		creds = ydb.WithAccessTokenCredentials(token)
	}
	if v, has := os.LookupEnv("YDB_ANONYMOUS_CREDENTIALS"); has && v == "1" {
		creds = ydb.WithAnonymousCredentials()
	}
	db, err := ydb.New(
		ctx,
		ydb.WithConnectionString(os.Getenv("YDB_CONNECTION_STRING")),
		ydb.WithDialTimeout(5*time.Second),
		ydb.WithBalancer(balancers.RandomChoice()),
		creds,
		ydb.WithSessionPoolSizeLimit(300),
		ydb.WithSessionPoolIdleThreshold(time.Second*5),
		ydbZap.WithTraces(log, trace.DetailsAll),
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close(ctx)
	}()

	wg := &sync.WaitGroup{}

	if concurrency, err := strconv.Atoi(os.Getenv("YDB_PREPARE_BENCH_DATA")); err == nil && concurrency > 0 {
		_ = upsertData(ctx, db.Table(), db.Name(), "series", concurrency)
	}

	concurrency := func() int {
		if concurrency, err := strconv.Atoi(os.Getenv("CONCURRENCY")); err != nil {
			return concurrency
		}
		return 300
	}()

	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			for {
				time.Sleep(time.Duration(rand.Int63n(int64(time.Second))))
				start := time.Now()
				count, err := scanSelect(
					ctx,
					db.Table(),
					db.Name(),
					rand.Int63n(25000),
				)
				log.Debug("scan select",
					zap.Duration("latency", time.Since(start)),
					zap.Uint64("count", count),
					zap.Error(err),
				)
			}
		}()
	}
	wg.Wait()
}

func upsertData(ctx context.Context, c table.Client, prefix, tableName string, concurrency int) (err error) {
	err = c.Do(
		ctx,
		func(ctx context.Context, s table.Session) (err error) {
			return s.CreateTable(ctx, path.Join(prefix, tableName),
				options.WithColumn("series_id", types.Optional(types.TypeUint64)),
				options.WithColumn("title", types.Optional(types.TypeUTF8)),
				options.WithColumn("series_info", types.Optional(types.TypeUTF8)),
				options.WithColumn("release_date", types.Optional(types.TypeUint64)),
				options.WithColumn("comment", types.Optional(types.TypeUTF8)),
				options.WithPrimaryKeyColumn("series_id"),
			)
		},
	)
	if err != nil {
		log.Error("create table", zap.Error(err))
	}
	rowsLen := 25000000
	batchSize := 1000
	wg := sync.WaitGroup{}
	sema := make(chan struct{}, concurrency)
	for shift := 0; shift < rowsLen; shift += batchSize {
		wg.Add(1)
		sema <- struct{}{}
		go func(prefix, tableName string, shift int) {
			defer func() {
				<-sema
				wg.Done()
			}()
			rows := make([]types.Value, 0, batchSize)
			for i := 0; i < batchSize; i++ {
				rows = append(rows, types.StructValue(
					types.StructFieldValue("series_id", types.Uint64Value(uint64(i+shift+3))),
					types.StructFieldValue("title", types.UTF8Value(fmt.Sprintf("series No. %d title", i+shift+3))),
					types.StructFieldValue("series_info", types.UTF8Value(fmt.Sprintf("series No. %d info", i+shift+3))),
					types.StructFieldValue("release_date", types.Uint64Value(uint64(time.Since(time.Unix(0, 0))/time.Hour/24))),
					types.StructFieldValue("comment", types.UTF8Value(fmt.Sprintf("series No. %d comment", i+shift+3))),
				))
			}
			err = c.Do(
				ctx,
				func(ctx context.Context, session table.Session) (err error) {
					return session.BulkUpsert(
						ctx,
						path.Join(prefix, tableName),
						types.ListValue(rows...),
					)
				},
			)
			if err == nil {
				log.Debug("bulk upserted", zap.Int("from", shift), zap.Int("to", shift+batchSize))
			} else {
				log.Error("bulk upsert failed", zap.Int("from", shift), zap.Int("to", shift+batchSize), zap.Error(err))
			}
		}(prefix, tableName, shift)
	}
	wg.Wait()
	return nil
}

func scanSelect(ctx context.Context, c table.Client, prefix string, limit int64) (count uint64, err error) {
	query := fmt.Sprintf(`
		PRAGMA TablePathPrefix("%s");
		$format = DateTime::Format("%%Y-%%m-%%d");
		SELECT
			series_id,
			title,
			$format(DateTime::FromSeconds(CAST(DateTime::ToSeconds(DateTime::IntervalFromDays(CAST(release_date AS Int16))) AS Uint32))) AS release_date
		FROM series LIMIT %d;`,
		prefix,
		limit,
	)
	err = c.Do(
		ctx,
		func(ctx context.Context, s table.Session) error {
			res, err := s.StreamExecuteScanQuery(
				ctx,
				query,
				table.NewQueryParameters(),
			)
			if err != nil {
				return err
			}
			var (
				id    *uint64
				title *string
				date  *[]byte
			)
			for res.NextResultSet(ctx, "series_id", "title", "release_date") {
				for res.NextRow() {
					count++
					err = res.Scan(&id, &title, &date)
					if err != nil {
						return err
					}
				}
			}
			return res.Err()
		},
	)
	return count, err
}
