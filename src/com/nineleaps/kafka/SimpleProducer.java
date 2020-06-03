package com.nineleaps.kafka;


import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

//Message send to KAFKA by using - Fire and Forget
public class SimpleProducer {

	public static void main(String[] args) {
		
		String topicName = "SimpleProducerTopic";
		String key = "key1";
		String value = "value-4";

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092, localhost:9093");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<>(props);

		ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName, key, value);

		producer.send(record);
 
		producer.close();

		System.out.println("SimpleProducer Completed.");
		
	}
}
