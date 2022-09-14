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

	///
	/// Topic reader
	///
	if details&trace.TopicReaderStreamLifeCycleEvents != 0 {
		logger := topicLogger.Named("reader").Named("lifecycle")

		t.OnReaderReconnect = func(startInfo trace.TopicReaderReconnectStartInfo) func(doneInfo trace.TopicReaderReconnectDoneInfo) {
			start := time.Now()

			logger.Debug("reconnecting")

			return func(doneInfo trace.TopicReaderReconnectDoneInfo) {
				logger.Info("reconnected",
					zap.Duration("latency", time.Since(start)),
					zap.Error(doneInfo.Error),
				)
			}
		}

		t.OnReaderReconnectRequest = func(info trace.TopicReaderReconnectRequestInfo) {
			logger.Debug("request reconnect", zap.NamedError("reason", info.Reason), zap.Bool("was_sent", info.WasSent))
		}

	}
	if details&trace.TopicReaderPartitionEvents != 0 {
		logger := topicLogger.Named("reader").Named("partition")
		t.OnReaderPartitionReadStartResponse = func(startInfo trace.TopicReaderPartitionReadStartResponseStartInfo) func(stopInfo trace.TopicReaderPartitionReadStartResponseDoneInfo) { //nolint:lll
			start := time.Now()
			logger.Debug("read partition response starting...",
				zap.String("topic", startInfo.Topic),
				zap.String("reader_connection_id", startInfo.ReaderConnectionID),
				zap.Int64("partition_id", startInfo.PartitionID),
				zap.Int64("partition_session_id", startInfo.PartitionSessionID))

			return func(doneInfo trace.TopicReaderPartitionReadStartResponseDoneInfo) {
				logger.Info("read partition response completed",
					zap.String("topic", startInfo.Topic),
					zap.String("reader_connection_id", startInfo.ReaderConnectionID),
					zap.Int64("partition_id", startInfo.PartitionID),
					zap.Int64("partition_session_id", startInfo.PartitionSessionID),

					zap.Duration("latency", time.Since(start)),
					zap.Int64p("commit_offset", doneInfo.CommitOffset),
					zap.Int64p("read_offset", doneInfo.ReadOffset),
					zap.Error(doneInfo.Error),
				)
			}
		}

		t.OnReaderPartitionReadStopResponse = func(startInfo trace.TopicReaderPartitionReadStopResponseStartInfo) func(trace.TopicReaderPartitionReadStopResponseDoneInfo) {
			start := time.Now()
			logger.Debug("reader partition stopping...",
				zap.String("reader_connection_id", startInfo.ReaderConnectionID),
				zap.String("topic", startInfo.Topic),
				zap.Int64("partition_id", startInfo.PartitionID),
				zap.Int64("partition_session_id", startInfo.PartitionSessionID),
				zap.Int64("committed_offset", startInfo.CommittedOffset),
				zap.Bool("graceful", startInfo.Graceful),
			)

			return func(doneInfo trace.TopicReaderPartitionReadStopResponseDoneInfo) {
				logInfoWarn(logger, doneInfo.Error,
					"reader partition stopped",
					zap.String("reader_connection_id", startInfo.ReaderConnectionID),
					zap.String("topic", startInfo.Topic),
					zap.Int64("partition_id", startInfo.PartitionID),
					zap.Int64("partition_session_id", startInfo.PartitionSessionID),
					zap.Int64("committed_offset", startInfo.CommittedOffset),
					zap.Bool("graceful", startInfo.Graceful),

					zap.Duration("latency", time.Since(start)),
				)
			}
		}
	}

	if details&trace.TopicReaderStreamEvents != 0 {
		logger := topicLogger.Named("reader").Named("stream")

		t.OnReaderCommit = func(startInfo trace.TopicReaderCommitStartInfo) func(doneInfo trace.TopicReaderCommitDoneInfo) {
			start := time.Now()
			logger.Debug("start committing...",
				zap.String("topic", startInfo.Topic),
				zap.Int64("partition_id", startInfo.PartitionID),
				zap.Int64("partition_session_id", startInfo.PartitionSessionID),
				zap.Int64("commit_start_offset", startInfo.StartOffset),
				zap.Int64("commit_end_offset", startInfo.EndOffset),
			)

			return func(doneInfo trace.TopicReaderCommitDoneInfo) {
				logDebugWarn(logger, doneInfo.Error, "committed",
					zap.String("topic", startInfo.Topic),
					zap.Int64("partition_id", startInfo.PartitionID),
					zap.Int64("partition_session_id", startInfo.PartitionSessionID),
					zap.Int64("commit_start_offset", startInfo.StartOffset),
					zap.Int64("commit_end_offset", startInfo.EndOffset),

					zap.Duration("latency", time.Since(start)),
				)
			}
		}

		t.OnReaderSendCommitMessage = func(startInfo trace.TopicReaderSendCommitMessageStartInfo) func(doneInfo trace.TopicReaderSendCommitMessageDoneInfo) {
			start := time.Now()
			logger.Debug("commit message sending...",
				zap.Int64s("partitions_id", startInfo.CommitsInfo.PartitionIDs()),
				zap.Int64s("partitions_session_id", startInfo.CommitsInfo.PartitionSessionIDs()),
			)

			return func(doneInfo trace.TopicReaderSendCommitMessageDoneInfo) {
				logDebugWarn(logger, doneInfo.Error, "commit message sent",
					zap.Int64s("partitions_id", startInfo.CommitsInfo.PartitionIDs()),
					zap.Int64s("partitions_session_id", startInfo.CommitsInfo.PartitionSessionIDs()),

					zap.Duration("latency", time.Since(start)),
				)
			}
		}

		t.OnReaderCommittedNotify = func(info trace.TopicReaderCommittedNotifyInfo) {
			logger.Debug("commit ack",
				zap.String("reader_connection_id", info.ReaderConnectionID),
				zap.String("topic", info.Topic),
				zap.Int64("partition_id", info.PartitionID),
				zap.Int64("partition_session_id", info.PartitionSessionID),
				zap.Int64("committed_offset", info.CommittedOffset),
			)
		}

		t.OnReaderClose = func(startInfo trace.TopicReaderCloseStartInfo) func(doneInfo trace.TopicReaderCloseDoneInfo) {
			start := time.Now()
			logger.Debug("stream closing...",
				zap.String("reader_connection_id", startInfo.ReaderConnectionID),
				zap.String("close_reason", startInfo.CloseReason.Error()),
			)

			return func(doneInfo trace.TopicReaderCloseDoneInfo) {
				logDebugWarn(logger, doneInfo.CloseError, "topic reader stream closed",
					zap.String("reader_connection_id", startInfo.ReaderConnectionID),
					zap.String("close_reason", startInfo.CloseReason.Error()),

					zap.Duration("latency", time.Since(start)),
				)
			}
		}

		t.OnReaderInit = func(startInfo trace.TopicReaderInitStartInfo) func(doneInfo trace.TopicReaderInitDoneInfo) {
			start := time.Now()
			logger.Debug("stream init starting...",
				zap.String("pre_init_reader_connection_id", startInfo.PreInitReaderConnectionID),
				zap.String("consumer", startInfo.InitRequestInfo.GetConsumer()),
				zap.Strings("topics", startInfo.InitRequestInfo.GetTopics()),
			)

			return func(doneInfo trace.TopicReaderInitDoneInfo) {
				logDebugWarn(logger, doneInfo.Error, "topic reader stream initialized",
					zap.String("pre_init_reader_connection_id", startInfo.PreInitReaderConnectionID),
					zap.String("consumer", startInfo.InitRequestInfo.GetConsumer()),
					zap.Strings("topics", startInfo.InitRequestInfo.GetTopics()),

					zap.Duration("latency", time.Since(start)),
					zap.String("reader_connection_id", doneInfo.ReaderConnectionID),
				)
			}
		}

		t.OnReaderError = func(info trace.TopicReaderErrorInfo) {
			logger.Warn("stream error",
				zap.String("reader_connection_id", info.ReaderConnectionID),
				zap.Error(info.Error),
			)
		}

		t.OnReaderUpdateToken = func(startInfo trace.OnReadUpdateTokenStartInfo) func(updateTokenInfo trace.OnReadUpdateTokenMiddleTokenReceivedInfo) func(doneInfo trace.OnReadStreamUpdateTokenDoneInfo) {
			start := time.Now()
			logger.Debug("token updating...",
				zap.String("reader_connection_id", startInfo.ReaderConnectionID),
			)

			return func(updateTokenInfo trace.OnReadUpdateTokenMiddleTokenReceivedInfo) func(doneInfo trace.OnReadStreamUpdateTokenDoneInfo) {
				logDebugWarn(logger, updateTokenInfo.Error, "got token",
					zap.String("reader_connection_id", startInfo.ReaderConnectionID),

					zap.Duration("latency", time.Since(start)),
					zap.Int("token_len", updateTokenInfo.TokenLen),
				)

				return func(doneInfo trace.OnReadStreamUpdateTokenDoneInfo) {
					logDebugWarn(logger, doneInfo.Error, "token updated on stream",
						zap.String("reader_connection_id", startInfo.ReaderConnectionID),

						zap.Int("token_len", updateTokenInfo.TokenLen),

						zap.Duration("latency", time.Since(start)),
					)
				}
			}
		}
	}

	if details&trace.TopicReaderMessageEvents != 0 {
		logger := topicLogger.Named("reader").Named("message")

		t.OnReaderSentDataRequest = func(info trace.TopicReaderSentDataRequestInfo) {
			logger.Debug("sent data request",
				zap.String("reader_connection_id", info.ReaderConnectionID),
				zap.Int("request_bytes", info.RequestBytes),
				zap.Int("local_capacity", info.LocalBufferSizeAfterSent),
			)
		}

		t.OnReaderReceiveDataResponse = func(startInfo trace.TopicReaderReceiveDataResponseStartInfo) func(doneInfo trace.TopicReaderReceiveDataResponseDoneInfo) {
			start := time.Now()
			partitionsCount, batchesCount, messagesCount := startInfo.DataResponse.GetPartitionBatchMessagesCounts()
			logger.Debug("data response received, process starting...",
				zap.String("reader_connection_id", startInfo.ReaderConnectionID),
				zap.Int("received_bytes", startInfo.DataResponse.GetBytesSize()),
				zap.Int("local_capacity", startInfo.LocalBufferSizeAfterReceive),
				zap.Int("partitions_count", partitionsCount),
				zap.Int("batches_count", batchesCount),
				zap.Int("messages_count", messagesCount),
			)

			return func(doneInfo trace.TopicReaderReceiveDataResponseDoneInfo) {
				logDebugWarn(logger, doneInfo.Error, "data response received and processed",
					zap.String("reader_connection_id", startInfo.ReaderConnectionID),
					zap.Int("received_bytes", startInfo.DataResponse.GetBytesSize()),
					zap.Int("local_capacity", startInfo.LocalBufferSizeAfterReceive),
					zap.Int("partitions_count", partitionsCount),
					zap.Int("batches_count", batchesCount),
					zap.Int("messages_count", messagesCount),

					zap.Duration("latency", time.Since(start)),
				)
			}
		}

		t.OnReaderReadMessages = func(startInfo trace.TopicReaderReadMessagesStartInfo) func(doneInfo trace.TopicReaderReadMessagesDoneInfo) {
			start := time.Now()
			logger.Debug("read messages called, waiting...",
				zap.Int("min_count", startInfo.MinCount),
				zap.Int("max_count", startInfo.MaxCount),
				zap.Int("local_capacity_before", startInfo.FreeBufferCapacity),
			)

			return func(doneInfo trace.TopicReaderReadMessagesDoneInfo) {
				logDebugInfo(logger, doneInfo.Error, "read messages returned",
					zap.Int("min_count", startInfo.MinCount),
					zap.Int("max_count", startInfo.MaxCount),
					zap.Int("local_capacity_before", startInfo.FreeBufferCapacity),

					zap.String("topic", doneInfo.Topic),
					zap.Int64("partition_id", doneInfo.PartitionID),
					zap.Int("messages_count", doneInfo.MessagesCount),
					zap.Int("local_capacity_after", doneInfo.FreeBufferCapacity),
					zap.Duration("latency", time.Since(start)),
				)
			}
		}

		t.OnReaderUnknownGrpcMessage = func(info trace.OnReadUnknownGrpcMessageInfo) {
			logger.Info("received unknown message",
				zap.String("reader_connection_id", info.ReaderConnectionID),
				zap.Error(info.Error),
			)
		}
	}

	///
	/// Topic writer
	///
	if details&trace.TopicWriterStreamLifeCycleEvents != 0 {
		logger := topicLogger.Named("writer").Named("lifecycle")
		t.OnWriterReconnect = func(startInfo trace.TopicWriterReconnectStartInfo) func(doneInfo trace.TopicWriterReconnectDoneInfo) {
			start := time.Now()
			logger.Debug("connect to topic writer stream starting...",
				zap.String("topic", startInfo.Topic),
				zap.String("producer_id", startInfo.ProducerID),
				zap.String("writer_instance_id", startInfo.WriterInstanceID),
				zap.Int("attempt", startInfo.Attempt),
			)
			return func(doneInfo trace.TopicWriterReconnectDoneInfo) {
				logDebugInfo(logger, doneInfo.Error, "connect to topic writer stream completed",
					zap.String("topic", startInfo.Topic),
					zap.String("producer_id", startInfo.ProducerID),
					zap.String("writer_instance_id", startInfo.WriterInstanceID),
					zap.Int("attempt", startInfo.Attempt),
					//
					zap.Duration("latency", time.Since(start)),
				)
			}
		}
		t.OnWriterInitStream = func(startInfo trace.TopicWriterInitStreamStartInfo) func(doneInfo trace.TopicWriterInitStreamDoneInfo) {
			start := time.Now()
			logger.Debug("init stream starting...",
				zap.String("topic", startInfo.Topic),
				zap.String("producer_id", startInfo.ProducerID),
				zap.String("writer_instance_id", startInfo.WriterInstanceID),
			)

			return func(doneInfo trace.TopicWriterInitStreamDoneInfo) {
				logDebugInfo(logger, doneInfo.Error, "init stream completed {topic:'%v', producer_id:'%v', writer_instance_id: '%v'",
					zap.String("topic", startInfo.Topic),
					zap.String("producer_id", startInfo.ProducerID),
					zap.String("writer_instance_id", startInfo.WriterInstanceID),
					//
					zap.Duration("latency", time.Since(start)),
					zap.String("session", doneInfo.SessionID),
				)
			}
		}
		t.OnWriterClose = func(startInfo trace.TopicWriterCloseStartInfo) func(doneInfo trace.TopicWriterCloseDoneInfo) {
			start := time.Now()
			logger.Debug("close topic writer starting... ",
				zap.String("writer_instance_id", startInfo.WriterInstanceID),
				zap.NamedError("reason", startInfo.Reason),
			)

			return func(doneInfo trace.TopicWriterCloseDoneInfo) {
				logDebugInfo(logger, doneInfo.Error, "close topic writer starting... {writer_instance_id: '%v', reason: '%v'",
					zap.String("writer_instance_id", startInfo.WriterInstanceID),
					zap.NamedError("reason", startInfo.Reason),
					//
					zap.Duration("latency", time.Since(start)),
				)
			}
		}
	}
	if details&trace.TopicWriterStreamEvents != 0 {
		logger := topicLogger.Named("writer").Named("stream")
		t.OnWriterCompressMessages = func(startInfo trace.TopicWriterCompressMessagesStartInfo) func(doneInfo trace.TopicWriterCompressMessagesDoneInfo) {
			start := time.Now()
			logger.Debug("compress message starting...",
				zap.String("writer_instance_id", startInfo.WriterInstanceID),
				zap.String("session_id", startInfo.SessionID),
				zap.Stringer("reason", startInfo.Reason),
				zap.Int32("codec", startInfo.Codec),
				zap.Int("messages_count", startInfo.MessagesCount),
				zap.Int64("first_seqno", startInfo.FirstSeqNo),
			)

			return func(doneInfo trace.TopicWriterCompressMessagesDoneInfo) {
				logDebugInfo(logger, doneInfo.Error, "compress message completed {writer_instance_id:'%v', session_id: '%v', reason: %v, codec: %v, messages_count: %v, first_seqno: %v}",
					zap.String("writer_instance_id", startInfo.WriterInstanceID),
					zap.String("session_id", startInfo.SessionID),
					zap.Stringer("reason", startInfo.Reason),
					zap.Int32("codec", startInfo.Codec),
					zap.Int("messages_count", startInfo.MessagesCount),
					zap.Int64("first_seqno", startInfo.FirstSeqNo),
					//
					zap.Duration("latency", time.Since(start)),
				)
			}
		}
		t.OnWriterSendMessages = func(startInfo trace.TopicWriterSendMessagesStartInfo) func(doneInfo trace.TopicWriterSendMessagesDoneInfo) {
			start := time.Now()
			logger.Debug("compress message starting...",
				zap.String("writer_instance_id", startInfo.WriterInstanceID),
				zap.String("session_id", startInfo.SessionID),
				zap.Int32("codec", startInfo.Codec),
				zap.Int("messages_count", startInfo.MessagesCount),
				zap.Int64("first_seqno", startInfo.FirstSeqNo),
			)

			return func(doneInfo trace.TopicWriterSendMessagesDoneInfo) {
				logDebugInfo(logger, doneInfo.Error, "compress message completed",
					zap.String("writer_instance_id", startInfo.WriterInstanceID),
					zap.String("session_id", startInfo.SessionID),
					zap.Int32("codec", startInfo.Codec),
					zap.Int("messages_count", startInfo.MessagesCount),
					zap.Int64("first_seqno", startInfo.FirstSeqNo),
					//
					zap.Duration("latency", time.Since(start)),
				)
			}
		}
		t.OnWriterReadUnknownGrpcMessage = func(info trace.TopicOnWriterReadUnknownGrpcMessageInfo) {
			logger.Info(
				"topic writer receive unknown message from server {writer_instance_id:'%v', session_id:'%v', error: '%v'}",
				zap.String("writer_instance_id", info.WriterInstanceID),
				zap.String("session_id", info.SessionID),
				zap.Error(info.Error),
			)
		}
	}

	return t
}
