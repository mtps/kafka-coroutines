package com.github.mtps.kafka.cli

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import com.github.mtps.kafka.AckedConsumerRecord
import com.github.mtps.kafka.UnAckedConsumerRecord
import com.github.mtps.kafka.kafkaFlow
import java.time.Duration
import java.util.UUID
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import mu.KotlinLogging
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory

val commonProps = { id: String? ->
	mapOf(
		CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
		CommonClientConfigs.CLIENT_ID_CONFIG to (id ?: UUID.randomUUID().toString()),
	)
}

val producerProps = mapOf(
	ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
	ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
	ProducerConfig.ACKS_CONFIG to "all",
	ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to true,
)

val consumerProps = mapOf(
	ConsumerConfig.MAX_POLL_RECORDS_CONFIG to 15,
	ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
	ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG to 10000,
	ConsumerConfig.GROUP_ID_CONFIG to "test-group",
	ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
	ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
)

fun logger(name: String): org.slf4j.Logger = LoggerFactory.getLogger(name)
var org.slf4j.Logger.level: Level
	get() = (this as Logger).level
	set(value) { (this as Logger).level = value }

fun producerProps(id: String) = commonProps(id) + producerProps
fun consumerProps(id: String) = commonProps(id) + consumerProps

fun main() {

	val log = KotlinLogging.logger {}
	val topics = listOf("test")

	logger("org.apache.kafka.clients").level = Level.INFO
	logger("org.apache.kafka.common.network").level = Level.WARN

	runBlocking {
		log.info("creating kafka channel")

		val kafkaFlowA = kafkaFlow<String, String>(consumerProps("test-stage-1"), setOf("test"))
			.receiveAsFlow()
			.map { it.withValue { "${it.value()}-another".let { it to it.length } } }
			.toTopic("another", producerProps("througher-producer"))

		val kafkaFlowB = kafkaFlow<String, String>(consumerProps("test-stage-2"), setOf("another"))
			.receiveAsFlow()

		val count = 10
		launch {
			kafkaFlowA.collect {
				// Context of main thread.
				log.info("recv from A: ${it.record.topic()} :: ${it.record.partition()} -> ${it.record.offset()}")
			}
		}

		launch {
			kafkaFlowB.collect {
				// Context of main thread.
				log.info("recv from B: ${it.record.topic()} :: ${it.record.partition()} -> ${it.record.offset()}")
				it.ack()
			}
		}

		launch {
			val producer = KafkaProducer(producerProps("source-producer"), StringSerializer(), StringSerializer())
			val c = AtomicInteger(0)
			while (true) {
				repeat(count) {
					val sent = producer.send(ProducerRecord("test", "key-${c.get()}", "value-$it")).asDeferred().await()
					log.info("sent $sent")
				}
				c.incrementAndGet()
				delay(5000)
			}
		}
		log.info("done!")
	}
}

fun <K, V> Flow<UnAckedConsumerRecord<K, V>>.toTopic(
	topic: String,
	producerConfig: Map<String, Any> = emptyMap(),
	producer: Producer<K, V> = KafkaProducer(producerConfig),
): Flow<AckedConsumerRecord<K, V>> {
	val log = KotlinLogging.logger {}
	return flow {
		collect { value ->
			val record = ProducerRecord(topic, null, value.record.key(), value.record.value(), value.record.headers())
			val metadata = producer.send(record).asDeferred().await()
			log.info("successfully sent ${metadata.topic()}-${metadata.partition()}@${metadata.offset()}")
			emit(value.ack())
		}
	}
}

suspend fun <T> Future<T>.asDeferred(timeout: Duration? = null, coroutineContext: CoroutineContext = Dispatchers.IO): Deferred<T> {
	return withContext(coroutineContext) {
		async {
			if (timeout == null) get()
			else get(timeout.toMillis(), TimeUnit.MILLISECONDS)
		}
	}
}

