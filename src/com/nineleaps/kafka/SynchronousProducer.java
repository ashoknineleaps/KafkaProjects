package com.nineleaps.kafka;

import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class SynchronousProducer {

	public static void main(String[] args) {

		String topicName = "SynchronousProducerTopic";
		String key = "key1";
		String value = "value-3";

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092, localhost:9093");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<>(props);

		ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName, key, value);

		try {
			Future<RecordMetadata> future = producer.send(record);

			RecordMetadata recordMetadata = future.get();

			System.out.println("Message send to be the Partition Number: " + recordMetadata.partition()
					+ " and offset: " + recordMetadata.offset());

			System.out.println("SynchronousProducer Completed with Success.");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("SynchronousProducer Failed with an exception");
		} finally {
			producer.close();
		}
	}
}
