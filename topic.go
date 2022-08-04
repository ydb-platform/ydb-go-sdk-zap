package zap

import (
	"go.uber.org/zap"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func Topic(log *zap.Logger, details trace.Details) trace.Topic {
	log = log.Named("ydb").Named("topic")
	t := trace.Topic{}

	if details&trace.TopicReaderStreamEvents != 0 {
		t.OnReaderConnect = func(info trace.TableReaderConnectStartInfo) func(trace.TableReaderConnectDoneInfo) {
			log.Debug("connecting")
			return func(info trace.TableReaderConnectDoneInfo) {
				log.Info("connected", zap.Error(info.Error))
			}
		}
		t.OnReaderReconnect = func(info trace.TableReaderReconnectStartInfo) func(trace.TableReaderReconnectDoneInfo) {
			log.Debug("reconnecting")

			return func(info trace.TableReaderReconnectDoneInfo) {
				log.Info("reconnected", zap.Error(info.Error))
			}
		}
		t.OnReaderReconnectRequest = func(info trace.TableReaderReconnectRequestInfo) {
			log.Debug("request reconnect", zap.NamedError("reason", info.Reason))
		}
		t.OnReaderPartitionReadStartResponse = func(info trace.TableReaderPartitionReadStartResponseStartInfo) func(trace.TableReaderPartitionReadStartResponseDoneInfo) { //nolint:lll
			logger := log.With(zap.String("topic", info.Topic),
				zap.Int64("partition_id", info.PartitionID),
				zap.Int64("partition_session_id", info.PartitionSessionID))
			logger.Debug("read partition responsing")

			return func(info trace.TableReaderPartitionReadStartResponseDoneInfo) {
				logger.Info("read partition response",
					zap.Int64p("commit_offset", info.CommitOffset),
					zap.Int64p("read_offset", info.ReadOffset),
					zap.Error(info.Error),
				)
			}
		}
	}
	return t
}
