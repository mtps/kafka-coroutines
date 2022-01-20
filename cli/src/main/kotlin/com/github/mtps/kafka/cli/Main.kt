package com.github.mtps.kafka.cli

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
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
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.transform
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory

val props = mapOf(
	ConsumerConfig.CLIENT_ID_CONFIG to UUID.randomUUID().toString(),
	ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
	ConsumerConfig.MAX_POLL_RECORDS_CONFIG to 100,
	ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG to 10000,
	ConsumerConfig.GROUP_ID_CONFIG to "test-group",
	ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
	ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
	ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
	ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
)

fun logger(name: String): org.slf4j.Logger = LoggerFactory.getLogger(name)
var org.slf4j.Logger.level: Level
	get() = (this as Logger).level
	set(value) { (this as Logger).level = value }

fun main() {

	val log = KotlinLogging.logger {}
	val topics = listOf("test")

	logger("org.apache.kafka.clients").level = Level.WARN
	logger("org.apache.kafka.common.network").level = Level.WARN

	runBlocking {
		log.info("creating kafka channel")
		launch {
			val db = mutableListOf<Pair<UUID, String>>()

			kafkaFlow<String, String>(props) { subscribe(topics) }
				.transform {
					// Mimic local storage.
					val newID = UUID.randomUUID()
					db += newID to "${it.record.key()}-${it.record.value()}"
					// Commit this receipt back to kafka.
					it.ack()

					emit(newID to it.record)
				}
				.collect {
					// Context of main thread
					log.info("recv: ${it.first} :: ${it.second.key()} -> ${it.second.value()}")
				}
		}

		log.info("launching producer")
		launch(Dispatchers.IO) {
			val producer = KafkaProducer(props, StringSerializer(), StringSerializer())
			val count = AtomicInteger(0)
			while (true) {
				repeat(10) {
					val sent = producer.send(ProducerRecord("test", "key-${count.get()}", "value-$it")).asDeferred().await()
					log.info("sent $sent")
				}
				count.incrementAndGet()
				delay(5000)
			}
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