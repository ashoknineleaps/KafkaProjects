package com.nineleaps.kafka.consumer;

import java.util.Calendar;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class RandomProducer {

	public static void main(String[] args) {
		
		String topicName = "RandomProducerTopic"; 
		String msg;
		
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092, localhost:9093");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		Producer<String, String> producer = new KafkaProducer<>(props);
		
		Random random = new Random();
		
		Calendar calendar = Calendar.getInstance();
		calendar.set(2020, 05, 25);
		
		try
		{
			while(true)
			{
				for(int i=0; i<100; i++)
				{
					msg = calendar.get(Calendar.YEAR)+"-"+calendar.get(Calendar.MONTH)+"-"+calendar.get(Calendar.DATE)+", "+ random.nextInt(1000);
					int partitionZero = 0;
					ProducerRecord<String, String> recordP0 = new ProducerRecord<String, String>(topicName, partitionZero,"key", msg);
					producer.send(recordP0);
					
					msg = calendar.get(Calendar.YEAR)+"-"+calendar.get(Calendar.MONTH)+"-"+calendar.get(Calendar.DATE)+", "+ random.nextInt(1000);
					int partitionOne = 1;
					ProducerRecord<String, String> recordP1 = new ProducerRecord<String, String>(topicName, partitionOne,"key", msg);
					producer.send(recordP1);
				}
				int amount = 1;
				calendar.add(Calendar.DATE, amount);
				System.out.println("Data sent for "+calendar.get(Calendar.YEAR)+"-"+calendar.get(Calendar.MONTH)+"-"+calendar.get(Calendar.DATE));
			}
		}
		catch (Exception e) {
			System.out.println("Intrupted: "+e.getMessage());
		}
		finally {
			producer.close();
		}
	}
}
