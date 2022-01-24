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
import kotlin.random.Random
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.cancellable
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.onStart
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.transform
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory

val props = mapOf(
	ConsumerConfig.CLIENT_ID_CONFIG to UUID.randomUUID().toString(),
	ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
	ConsumerConfig.MAX_POLL_RECORDS_CONFIG to 15,
	ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
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

		val db = mutableListOf<Pair<UUID, String>>()
		val kafkaFlow = kafkaFlow<String, String>(props) { subscribe(topics) }
			.receiveAsFlow()
			.transform {
				// Mimic local storage.
				val newID = UUID.randomUUID()
				db += newID to "${it.record.key()}-${it.record.value()}"

				// Random delay.
				// delay(Duration.ofMillis(Random.nextLong(500)).toMillis())

				// Commit this receipt back to kafka.
				emit(newID to it.ack())
			}

		val count = 100
		launch {
			kafkaFlow.collect {
				// Context of main thread
				log.info("recv: ${it.first} :: ${it.second.record.key()} -> ${it.second.record.value()}")
			}
		}

		launch {
			val producer = KafkaProducer(props, StringSerializer(), StringSerializer())
			val c = AtomicInteger(0)
			repeat(count) {
				val sent = producer.send(ProducerRecord("test", "key-${c.get()}", "value-$it")).asDeferred().await()
				log.info("sent $sent")
			}
			c.incrementAndGet()
		}
		log.info("done!")
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