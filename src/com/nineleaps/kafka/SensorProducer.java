package com.nineleaps.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class SensorProducer {

	public static void main(String[] args) {
		
		String topicName = "SensorTopic";

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092, localhost:9093");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("partitioner.class", "com.nineleaps.kafka.SensorPartitioner");
		props.put("speed.sensor.name", "TSS");
		
		Producer<String, String> producer = new KafkaProducer<>(props);
		
		for(int i=0; i < 10; i++)
		{
			ProducerRecord<String, String> record = new ProducerRecord<>(topicName, "SSP"+i, "500"+i);
			producer.send(record);
		}
		
		for(int i=0; i < 10; i++)
		{
			ProducerRecord<String, String> record = new ProducerRecord<>(topicName, "TSS", "500"+i);
			producer.send(record);
		}
		
		producer.close();
		
		System.out.println("SensorProducer Completed.");
	}
}
