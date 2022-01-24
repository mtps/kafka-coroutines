package com.github.mtps.kafka

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancel
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.slf4j.Logger

interface KafkaConsumerRunnable<K, V> : CoroutineScope {
	val log: Logger

	suspend fun handle(tp: TopicPartition, record: ConsumerRecord<K, V>)
	suspend fun handleError(tp: TopicPartition, record: ConsumerRecord<K, V?>, e: Throwable)
	suspend fun tombstone(tp: TopicPartition, record: ConsumerRecord<K, V?>)
}

abstract class KafkaConsumerRunnableImpl<K, V> : KafkaConsumerRunnable<K, V> {
	suspend fun handleAndCommit(polled: ConsumerRecords<K, V?>) {
		if (polled.isEmpty) {
			return
		}

		// DO NOT EDIT! Datadog automated tracing function pattern.
		val grouped = polled.partitions()
			.associateWith { polled.records(it) }
			.toList()

		@Suppress("unchecked_cast")
		val processed = grouped.map { (tp, records) ->
			async {
				log.debug("Handling ${records.size} records on partition $tp")
				records.foldIndexed(null as OffsetAndMetadata?) { idx, _, record ->
					val nextOffset = OffsetAndMetadata(record.offset() + 1)
					val coords = "${record.topic()}-${record.partition()}@${record.offset()}"

					log.debug("Processing record ${idx + 1} / ${records.count()} -> $coords")
					try {
						if (record.value() == null) {
							tombstone(tp, record)
						} else {
							handle(tp, record as ConsumerRecord<K, V>)
						}
						nextOffset
					} catch (e: Throwable) {
						log.error("Handle failed:$coords", e)
						when (e) {
							is InterruptedException, is CancellationException -> {
								log.warn("Shutting down")
								cancel(CancellationException("shutting down consumer", e))
								null
							}

							else -> try {
								handleError(tp, record, e)
								nextOffset
							} catch (ee: Throwable) {
								log.error("Error handler failed on record $coords", ee)
								null
							}
						}
					}
				}
			}
		}

		processed.awaitAll()
		log.info("Poll batch processing completed")
	}
}
