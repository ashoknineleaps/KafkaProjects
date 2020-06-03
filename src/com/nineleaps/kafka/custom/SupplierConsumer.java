package com.nineleaps.kafka.custom;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class SupplierConsumer {

	public static void main(String[] args) {

		String topicName = "SupplierTopic";
		String groupName = "SupplierTopicGroup";

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092,localhost:9093");
		props.put("group.id", groupName);
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "com.nineleaps.kafka.custom.SupplierDeserializer");

		KafkaConsumer<String, Supplier> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList(topicName));

		while (true) {
			long timeout = 100;
			@SuppressWarnings("deprecation")
			ConsumerRecords<String, Supplier> records = consumer.poll(timeout);

			for (ConsumerRecord<String, Supplier> record : records) {
				System.out.println("Supplier Id: " + String.valueOf(record.value().getSupplierId())
						+ ", Suupplier Name: " + record.value().getSupplierName() + ", Supplier Start Date: "
						+ record.value().getSupplierStartDate().toString());
			}
		}
	}
}
