package com.github.mtps.kafka

import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Runnable
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.slf4j.Logger

private val DEFAULT_POLL_INTERVAL = Duration.ofMillis(500)

private fun <T, L : Iterable<T>> L.ifEmpty(block: () -> L): L = if (count() == 0) block() else this

class KafkaFlow<K, V>(
	consumerProperties: Map<String, Any>,
	private val pollInterval: Duration = DEFAULT_POLL_INTERVAL,
	private val name: String = "kafka-channel",
	private val init: Consumer<K, V>.() -> Unit,
) {

	companion object {
		private val counter = AtomicInteger(0)
	}

	private val log = KotlinLogging.logger {}
	private val channel: Channel<AckConsumerRecord<K, V>> = Channel(Channel.UNLIMITED)
	private val consumer = KafkaConsumer<K, V>(consumerProperties)

	private val thread = thread(name = "$name-${counter.getAndIncrement()}", block = { run() }, isDaemon = true, start = false)

	@OptIn(ExperimentalCoroutinesApi::class)
	fun run() {
		consumer.init()

		runBlocking {
			try {
				while (!channel.isClosedForSend) {
					val polled = consumer.poll(Duration.ZERO).ifEmpty { consumer.poll(pollInterval) }

					// Purposely allow call to throw, allowing restart strategies.
					val polledCount = polled.count()
					if (polledCount == 0) {
						continue
					}

					val ackChannel = Channel<Pair<TopicPartition, OffsetAndMetadata>>(capacity = polled.count())

					for (it in polled) {
						channel.send(AckConsumerRecordImpl(it, ackChannel))
					}

					if (polledCount != 0) {
						val count = AtomicInteger(polledCount)
						while (true) {
							if (count.get() == 0) {
								break
							}

							val it = ackChannel.receive()
							log.info("ack:$it")
							consumer.commitSync(mapOf(it))
							count.getAndDecrement()
						}
					}
					ackChannel.close()
				}
			} catch (e: Throwable) {
				cancel(CancellationException("thrown", e))
			} finally {
				log.info("shutting down consumer thread")
				consumer.unsubscribe()
				consumer.close()
				channel.cancel(CancellationException("consumer shut down"))
			}
		}
	}

	fun stop() {
		log.info("shutting down consumer")
		channel.close()
		consumer.wakeup()
	}

	fun start() {
		if (!thread.isAlive) {
			thread.start()
		}
	}

	fun channel(): Channel<AckConsumerRecord<K, V>> = channel
}

@OptIn(ExperimentalCoroutinesApi::class)
fun <K, V> kafkaFlow(
	consumerProperties: Map<String, Any>,
	pollInterval: Duration = DEFAULT_POLL_INTERVAL,
	name: String = "kafka-channel",
	init: Consumer<K, V>.() -> Unit,
): Flow<AckConsumerRecord<K, V>> = flow {
	emitAll(
		KafkaFlow(consumerProperties, pollInterval, name, init).also {
			it.start()

			Runtime.getRuntime().addShutdownHook(Thread {
				it.stop()
			})
		}.channel()
	)

}

typealias KafkaConsumerHandler<K, V> = (ConsumerRecord<K, V>) -> Unit
typealias KafkaConsumerErrorHandler<K, V> = (ConsumerRecord<K, V>, Throwable) -> Unit

data class KafkaRunnerOptions<K, V>(
	val topics: Set<String> = emptySet(),
	val handler: KafkaConsumerHandler<K, V>
)

interface KafkaConsumerRunnable<K, V> : CoroutineScope {
	val log: Logger

	suspend fun handle(tp: TopicPartition, record: ConsumerRecord<K, V>)
	suspend fun handleError(tp: TopicPartition, record: ConsumerRecord<K, V?>, e: Throwable)
	suspend fun tombstone(tp: TopicPartition, record: ConsumerRecord<K, V?>)

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

abstract class KafkaRunner<K, V>(
	private val consumer: Consumer<K, V>,
	private val onError: (ConsumerRecord<K, V>, Throwable) -> Unit = { _, _ -> },
	private val onReceive: (AckConsumerRecord<K, V>) -> Unit = { _ -> },
) {
	abstract fun start()
	abstract fun stop()
}

interface AckConsumerRecord<K, V> {
	val record: ConsumerRecord<K, V>
	suspend fun ack()
}

class AckConsumerRecordImpl<K, V>(
	override val record: ConsumerRecord<K, V>,
	private val channel: SendChannel<Pair<TopicPartition, OffsetAndMetadata>>,
) : AckConsumerRecord<K, V> {

	private val log = KotlinLogging.logger {}

	override suspend fun ack() {
		val tp = TopicPartition(record.topic(), record.partition())
		val commit = tp to OffsetAndMetadata(record.offset() + 1)
		channel.send(commit)
	}
}