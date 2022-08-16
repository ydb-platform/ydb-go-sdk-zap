//nolint:lll
package zap

import (
	"time"

	"go.uber.org/zap"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func Topic(topicLogger *zap.Logger, details trace.Details, opts ...option) trace.Topic {
	topicLogger = topicLogger.Named("ydb").Named("topic")
	t := trace.Topic{}

	if details&trace.TopicReaderStreamLifeCycleEvents != 0 {
		log := topicLogger.Named("reader").Named("lifecycle")

		t.OnReaderReconnect = func(startInfo trace.TopicReaderReconnectStartInfo) func(doneInfo trace.TopicReaderReconnectDoneInfo) {
			start := time.Now()

			log.Debug("reconnecting")

			return func(doneInfo trace.TopicReaderReconnectDoneInfo) {
				log.Info("reconnected",
					zap.Duration("latency", time.Since(start)),
					zap.Error(doneInfo.Error),
				)
			}
		}

		t.OnReaderReconnectRequest = func(info trace.TopicReaderReconnectRequestInfo) {
			log.Debug("request reconnect", zap.NamedError("reason", info.Reason), zap.Bool("was_sent", info.WasSent))
		}

	}
	if details&trace.TopicReaderPartitionEvents != 0 {
		log := topicLogger.Named("reader").Named("partition")
		t.OnReaderPartitionReadStartResponse = func(startInfo trace.TopicReaderPartitionReadStartResponseStartInfo) func(stopInfo trace.TopicReaderPartitionReadStartResponseDoneInfo) { //nolint:lll
			start := time.Now()
			startLogger := log.With(zap.String("topic", startInfo.Topic),
				zap.String("reader_connection_id", startInfo.ReaderConnectionID),
				zap.Int64("partition_id", startInfo.PartitionID),
				zap.Int64("partition_session_id", startInfo.PartitionSessionID))
			startLogger.Debug("read partition response starting...")

			return func(doneInfo trace.TopicReaderPartitionReadStartResponseDoneInfo) {
				startLogger.Info("read partition response completed",
					zap.Duration("latency", time.Since(start)),
					zap.Int64p("commit_offset", doneInfo.CommitOffset),
					zap.Int64p("read_offset", doneInfo.ReadOffset),
					zap.Error(doneInfo.Error),
				)
			}
		}

		t.OnReaderPartitionReadStopResponse = func(startInfo trace.TopicReaderPartitionReadStopResponseStartInfo) func(trace.TopicReaderPartitionReadStopResponseDoneInfo) {
			start := time.Now()
			startLogger := log.With(
				zap.String("reader_connection_id", startInfo.ReaderConnectionID),
				zap.String("topic", startInfo.Topic),
				zap.Int64("partition_id", startInfo.PartitionID),
				zap.Int64("partition_session_id", startInfo.PartitionSessionID),
				zap.Int64("committed_offset", startInfo.CommittedOffset),
				zap.Bool("graceful", startInfo.Graceful),
			)

			startLogger.Debug("reader partition stopping")
			return func(doneInfo trace.TopicReaderPartitionReadStopResponseDoneInfo) {
				logInfoWarn(startLogger, doneInfo.Error, "reader partition stopped",
					zap.Duration("latency", time.Since(start)),
				)
			}
		}
	}

	if details&trace.TopicReaderStreamEvents != 0 {
		log := topicLogger.Named("reader").Named("stream")

		t.OnReaderCommit = func(startInfo trace.TopicReaderCommitStartInfo) func(doneInfo trace.TopicReaderCommitDoneInfo) {
			start := time.Now()
			startLogger := log.With(
				zap.String("topic", startInfo.Topic),
				zap.Int64("partition_id", startInfo.PartitionID),
				zap.Int64("partition_session_id", startInfo.PartitionSessionID),
				zap.Int64("commit_start_offset", startInfo.StartOffset),
				zap.Int64("commit_end_offset", startInfo.EndOffset),
			)

			startLogger.Debug("start committing...")

			return func(doneInfo trace.TopicReaderCommitDoneInfo) {
				logDebugWarn(startLogger, doneInfo.Error, "committed",
					zap.Duration("latency", time.Since(start)),
				)
			}
		}

		t.OnReaderSendCommitMessage = func(startInfo trace.TopicReaderSendCommitMessageStartInfo) func(doneInfo trace.TopicReaderSendCommitMessageDoneInfo) {
			start := time.Now()
			startLogger := log.With(
				zap.Int64s("partitions_id", startInfo.CommitsInfo.PartitionIDs()),
				zap.Int64s("partitions_session_id", startInfo.CommitsInfo.PartitionSessionIDs()),
			)

			startLogger.Debug("commit message sending...")
			return func(doneInfo trace.TopicReaderSendCommitMessageDoneInfo) {
				logDebugWarn(startLogger, doneInfo.Error, "commit message sent",
					zap.Duration("latency", time.Since(start)),
				)
			}
		}

		t.OnReaderCommittedNotify = func(info trace.TopicReaderCommittedNotifyInfo) {
			log.Debug("commit ack",
				zap.String("reader_connection_id", info.ReaderConnectionID),
				zap.String("topic", info.Topic),
				zap.Int64("partition_id", info.PartitionID),
				zap.Int64("partition_session_id", info.PartitionSessionID),
				zap.Int64("committed_offset", info.CommittedOffset),
			)
		}

		t.OnReaderClose = func(startInfo trace.TopicReaderCloseStartInfo) func(doneInfo trace.TopicReaderCloseDoneInfo) {
			start := time.Now()
			startLogger := log.With(
				zap.String("reader_connection_id", startInfo.ReaderConnectionID),
				zap.String("close_reason", startInfo.CloseReason.Error()),
			)
			startLogger.Debug("stream closing")

			return func(doneInfo trace.TopicReaderCloseDoneInfo) {
				logDebugWarn(startLogger, doneInfo.CloseError, "topic reader stream closed",
					zap.Duration("latency", time.Since(start)),
				)
			}
		}

		t.OnReaderInit = func(startInfo trace.TopicReaderInitStartInfo) func(doneInfo trace.TopicReaderInitDoneInfo) {
			start := time.Now()
			startLogger := log.With(
				zap.String("pre_init_reader_connection_id", startInfo.PreInitReaderConnectionID),
				zap.String("consumer", startInfo.InitRequestInfo.GetConsumer()),
				zap.Strings("topics", startInfo.InitRequestInfo.GetTopics()),
			)
			startLogger.Debug("stream init starting...")

			return func(doneInfo trace.TopicReaderInitDoneInfo) {
				logDebugWarn(startLogger, doneInfo.Error, "topic reader stream initialized",
					zap.Duration("latency", time.Since(start)),
					zap.String("reader_connection_id", doneInfo.ReaderConnectionID),
				)
			}
		}

		t.OnReaderError = func(info trace.TopicReaderErrorInfo) {
			log.Warn("stream error",
				zap.String("reader_connection_id", info.ReaderConnectionID),
				zap.Error(info.Error),
			)
		}

		t.OnReaderUpdateToken = func(startInfo trace.OnReadUpdateTokenStartInfo) func(updateTokenInfo trace.OnReadUpdateTokenMiddleTokenReceivedInfo) func(doneInfo trace.OnReadStreamUpdateTokenDoneInfo) {
			start := time.Now()
			startLogger := log.With(
				zap.String("reader_connection_id", startInfo.ReaderConnectionID),
			)
			startLogger.Debug("token updating...")

			return func(updateTokenInfo trace.OnReadUpdateTokenMiddleTokenReceivedInfo) func(doneInfo trace.OnReadStreamUpdateTokenDoneInfo) {
				logDebugWarn(startLogger, updateTokenInfo.Error, "got token",
					zap.Duration("latency", time.Since(start)),
					zap.Int("token_len", updateTokenInfo.TokenLen),
				)

				return func(doneInfo trace.OnReadStreamUpdateTokenDoneInfo) {
					logDebugWarn(startLogger, doneInfo.Error, "token updated on stream",
						zap.Duration("latency", time.Since(start)),
					)
				}
			}
		}
	}

	if details&trace.TopicReaderMessageEvents != 0 {
		log := topicLogger.Named("reader").Named("message")

		t.OnReaderSentDataRequest = func(info trace.TopicReaderSentDataRequestInfo) {
			log.Debug("sent data request",
				zap.String("reader_connection_id", info.ReaderConnectionID),
				zap.Int("request_bytes", info.RequestBytes),
				zap.Int("local_capacity", info.LocalBufferSizeAfterSent),
			)
		}

		t.OnReaderReceiveDataResponse = func(startInfo trace.TopicReaderReceiveDataResponseStartInfo) func(doneInfo trace.TopicReaderReceiveDataResponseDoneInfo) {
			start := time.Now()
			partitionsCount, batchesCount, messagesCount := startInfo.DataResponse.GetPartitionBatchMessagesCounts()
			startLogger := log.With(
				zap.String("reader_connection_id", startInfo.ReaderConnectionID),
				zap.Int("received_bytes", startInfo.DataResponse.GetBytesSize()),
				zap.Int("local_capacity", startInfo.LocalBufferSizeAfterReceive),
				zap.Int("partitions_count", partitionsCount),
				zap.Int("batches_count", batchesCount),
				zap.Int("messages_count", messagesCount),
			)
			startLogger.Debug("data response received, process starting...")

			return func(doneInfo trace.TopicReaderReceiveDataResponseDoneInfo) {
				logDebugWarn(startLogger, doneInfo.Error, "data response received and processed",
					zap.Duration("latency", time.Since(start)),
				)
			}
		}

		t.OnReaderReadMessages = func(startInfo trace.TopicReaderReadMessagesStartInfo) func(doneInfo trace.TopicReaderReadMessagesDoneInfo) {
			start := time.Now()
			startLogger := log.With(
				zap.Int("min_count", startInfo.MinCount),
				zap.Int("max_count", startInfo.MaxCount),
				zap.Int("local_capacity_before", startInfo.FreeBufferCapacity),
			)
			startLogger.Debug("read messages called, waiting...")

			return func(doneInfo trace.TopicReaderReadMessagesDoneInfo) {
				logDebugInfo(startLogger, doneInfo.Error, "read messages returned",
					zap.String("topic", doneInfo.Topic),
					zap.Int64("partition_id", doneInfo.PartitionID),
					zap.Int("messages_count", doneInfo.MessagesCount),
					zap.Int("local_capacity_after", doneInfo.FreeBufferCapacity),
					zap.Duration("latency", time.Since(start)),
				)
			}
		}

		t.OnReaderUnknownGrpcMessage = func(info trace.OnReadUnknownGrpcMessageInfo) {
			log.Info("received unknown message",
				zap.String("reader_connection_id", info.ReaderConnectionID),
				zap.Error(info.Error),
			)
		}
	}
	return t
}
