package com.nineleaps.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class AsynchronousProducer {

	public static void main(String[] args) {
		
		String topicName = "AsynchronousProducerTopic";
		String key = "key1";
		String value = "value-3";

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092, localhost:9093");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<>(props);

		ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName, key, value);
		
		producer.send(record, new MyProducerCallback());
		
		System.out.println("AsynchronousProducer call Completed.");
		
		producer.close();
		
	}
}
