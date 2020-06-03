package com.nineleaps.kafka.custom;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class SupplierProducer {

	public static void main(String[] args) throws ParseException {
		
		String topicName = "SupplierTopic";
		
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092, localhost:9093");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "com.nineleaps.kafka.custom.SupplierSerializer");
		
		Producer<String, Supplier> producer = new KafkaProducer<>(props);
		
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		Supplier supplierFlipkart = new Supplier(101, "Paytm", df.parse("2020-05-26"));
		Supplier supplierAmazon = new Supplier(102, "eBay", df.parse("2020-04-25"));
		
		//Synchronous send message
		ProducerRecord<String, Supplier> flipkartRecord = new ProducerRecord<String, Supplier>(topicName, supplierFlipkart);
		producer.send(flipkartRecord);
		
		ProducerRecord<String, Supplier> amazonRecord = new ProducerRecord<String, Supplier>(topicName, supplierAmazon);
		producer.send(amazonRecord);
		
		System.out.println("SupplierProducer Completed.");
		
		producer.close();
		
	}
}
