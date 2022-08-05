//nolint:lll
package zap

import (
	"time"

	"go.uber.org/zap"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func Topic(topicLogger *zap.Logger, details trace.Details) trace.Topic {
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
			log.Debug("request reconnect", zap.NamedError("reason", info.Reason))
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

		t.OnReaderStreamCommit = func(startInfo trace.TopicReaderStreamCommitStartInfo) func(doneInfo trace.TopicReaderStreamCommitDoneInfo) {
			start := time.Now()
			startLogger := log.With(
				zap.String("topic", startInfo.Topic),
				zap.Int64("partition_id", startInfo.PartitionID),
				zap.Int64("partition_session_id", startInfo.PartitionSessionID),
				zap.Int64("commit_start_offset", startInfo.StartOffset),
				zap.Int64("commit_end_offset", startInfo.EndOffset),
			)
			startLogger.Debug("start committing...")

			return func(doneInfo trace.TopicReaderStreamCommitDoneInfo) {
				logDebugWarn(startLogger, doneInfo.Error, "committed",
					zap.Duration("latency", time.Since(start)),
				)
			}
		}

		t.OnReaderStreamSendCommitMessage = func(startInfo trace.TopicReaderStreamSendCommitMessageStartInfo) func(doneInfo trace.TopicReaderStreamSendCommitMessageDoneInfo) {
			start := time.Now()
			startLogger := log.With(
				zap.Int64s("partitions_id", startInfo.CommitsInfo.PartitionIDs()),
				zap.Int64s("partitions_session_id", startInfo.CommitsInfo.PartitionSessionIDs()),
			)
			startLogger.Debug("commit message sending...")
			return func(doneInfo trace.TopicReaderStreamSendCommitMessageDoneInfo) {
				logDebugWarn(startLogger, doneInfo.Error, "commit message sent",
					zap.Duration("latency", time.Since(start)),
				)
			}
		}

		t.OnReaderStreamCommittedNotify = func(info trace.TopicReaderStreamCommittedNotifyInfo) {
			log.Debug("commit ack",
				zap.String("reader_connection_id", info.ReaderConnectionID),
				zap.String("topic", info.Topic),
				zap.Int64("partition_id", info.PartitionID),
				zap.Int64("partition_session_id", info.PartitionSessionID),
				zap.Int64("committed_offset", info.CommittedOffset),
			)
		}

		t.OnReaderStreamClose = func(startInfo trace.TopicReaderStreamCloseStartInfo) func(doneInfo trace.TopicReaderStreamCloseDoneInfo) {
			start := time.Now()
			startLogger := log.With(
				zap.String("reader_connection_id", startInfo.ReaderConnectionID),
				zap.String("close_reason", startInfo.CloseReason.Error()),
			)
			startLogger.Debug("stream closing")

			return func(doneInfo trace.TopicReaderStreamCloseDoneInfo) {
				logDebugWarn(startLogger, doneInfo.CloseError, "topic reader stream closed",
					zap.Duration("latency", time.Since(start)),
				)
			}
		}

		t.OnReaderStreamInit = func(startInfo trace.TopicReaderStreamInitStartInfo) func(doneInfo trace.TopicReaderStreamInitDoneInfo) {
			start := time.Now()
			startLogger := log.With(
				zap.String("pre_init_reader_connection_id", startInfo.PreInitReaderConnectionID),
				zap.String("consumer", startInfo.InitRequestInfo.GetConsumer()),
				zap.Strings("topics", startInfo.InitRequestInfo.GetTopics()),
			)
			startLogger.Debug("stream init starting...")

			return func(doneInfo trace.TopicReaderStreamInitDoneInfo) {
				logDebugWarn(startLogger, doneInfo.Error, "topic reader stream initialized",
					zap.Duration("latency", time.Since(start)),
					zap.String("reader_connection_id", doneInfo.ReaderConnectionID),
				)
			}
		}

		t.OnReaderStreamError = func(info trace.TopicReaderStreamErrorInfo) {
			log.Warn("stream error",
				zap.String("reader_connection_id", info.ReaderConnectionID),
				zap.Error(info.Error),
			)
		}

		t.OnReaderStreamUpdateToken = func(startInfo trace.OnReadStreamUpdateTokenStartInfo) func(updateTokenInfo trace.OnReadStreamUpdateTokenMiddleTokenReceivedInfo) func(doneInfo trace.OnReadStreamUpdateTokenDoneInfo) {
			start := time.Now()
			startLogger := log.With(
				zap.String("reader_connection_id", startInfo.ReaderConnectionID),
			)
			startLogger.Debug("token updating...")

			return func(updateTokenInfo trace.OnReadStreamUpdateTokenMiddleTokenReceivedInfo) func(doneInfo trace.OnReadStreamUpdateTokenDoneInfo) {
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

		t.OnReaderStreamSentDataRequest = func(info trace.TopicReaderStreamSentDataRequestInfo) {
			log.Debug("sent data request",
				zap.String("reader_connection_id", info.ReaderConnectionID),
				zap.Int("request_bytes", info.RequestBytes),
				zap.Int("local_capacity", info.LocalBufferSizeAfterSent),
			)
		}

		t.OnReaderStreamReceiveDataResponse = func(startInfo trace.TopicReaderStreamReceiveDataResponseStartInfo) func(doneInfo trace.TopicReaderStreamReceiveDataResponseDoneInfo) {
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

			return func(doneInfo trace.TopicReaderStreamReceiveDataResponseDoneInfo) {
				logDebugWarn(startLogger, doneInfo.Error, "data response received and processed",
					zap.Duration("latency", time.Since(start)),
				)
			}
		}

		t.OnReaderStreamReadMessages = func(startInfo trace.TopicReaderStreamReadMessagesStartInfo) func(doneInfo trace.TopicReaderStreamReadMessagesDoneInfo) {
			start := time.Now()
			startLogger := log.With(
				zap.Int("min_count", startInfo.MinCount),
				zap.Int("max_count", startInfo.MaxCount),
				zap.Int("local_capacity_before", startInfo.FreeBufferCapacity),
			)
			startLogger.Debug("read messages called, waiting...")

			return func(doneInfo trace.TopicReaderStreamReadMessagesDoneInfo) {
				logDebugInfo(startLogger, doneInfo.Error, "read messages returned",
					zap.String("topic", doneInfo.Topic),
					zap.Int64("partition_id", doneInfo.PartitionID),
					zap.Int("messages_count", doneInfo.MessagesCount),
					zap.Int("local_capacity_after", doneInfo.FreeBufferCapacity),
					zap.Duration("latency", time.Since(start)),
				)
			}
		}

		t.OnReaderStreamUnknownGrpcMessage = func(info trace.OnReadStreamUnknownGrpcMessageInfo) {
			log.Info("received unknown message",
				zap.String("reader_connection_id", info.ReaderConnectionID),
				zap.Error(info.Error),
			)
		}
	}
	return t
}
