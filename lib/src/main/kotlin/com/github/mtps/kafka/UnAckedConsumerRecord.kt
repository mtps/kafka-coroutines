package com.github.mtps.kafka

import java.time.Duration
import kotlinx.coroutines.channels.SendChannel
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

interface UnAckedConsumerRecord<K, V> {
	val record: ConsumerRecord<K, V>
	suspend fun ack(): AckedConsumerRecord<K, V>
}

class UnAckedConsumerRecordImpl<K, V>(
	override val record: ConsumerRecord<K, V>,
	private val channel: SendChannel<CommitConsumerRecord<K, V>>,
	private val start: Long
) : UnAckedConsumerRecord<K, V> {

	override suspend fun ack(): AckedConsumerRecord<K, V> {
		val tp = TopicPartition(record.topic(), record.partition())
		val commit = OffsetAndMetadata(record.offset() + 1)
		val time = System.currentTimeMillis() - start
		channel.send(CommitConsumerRecordImpl(Duration.ofMillis(time), tp, commit))

		return AckedConsumerRecordImpl(record)
	}
}