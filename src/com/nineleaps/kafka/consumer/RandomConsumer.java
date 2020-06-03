package com.nineleaps.kafka.consumer;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class RandomConsumer {

	public static void main(String[] args) {
		
		String topicName = "RandomProducerTopic";
		KafkaConsumer<String, String> consumer = null;
		String groupName = "RG";
		
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092, localhost:9093");
		props.put("group.id", groupName);
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("enable.auto.commit", "false"); // Manual Commit
		
		consumer = new KafkaConsumer<>(props);
		
		RebalanceListener rebalanceListener = new RebalanceListener(consumer);
		
		consumer.subscribe(Arrays.asList(topicName), rebalanceListener);
		
		try
		{
			while(true)
			{
				long timeout = 100;
				ConsumerRecords<String, String> records = consumer.poll(timeout);
				
				for(ConsumerRecord<String, String> record : records)
				{
					rebalanceListener.addOffset(topicName, record.partition(), record.offset());
				}
			}
		}
		catch (Exception e) {
			System.out.println("RandomConsumer Failed..."+e.getMessage());
			e.printStackTrace();
		}
		finally {
			consumer.close();
		}
	}
}
