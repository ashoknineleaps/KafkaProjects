package com.nineleaps.kafka.consumer;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public class RebalanceListener implements ConsumerRebalanceListener {

	private KafkaConsumer<String, String> consumer;
	private Map<TopicPartition, OffsetAndMetadata> currentOffset = new HashMap<>();
	
	public RebalanceListener(KafkaConsumer<String, String> consumer)
	{
		this.consumer = consumer;
	}
	
	public void addOffset(String topic, int partition, long offset)
	{
		currentOffset.put(new TopicPartition(topic, partition), new OffsetAndMetadata(offset, "Commit"));
	}
	
	public Map<TopicPartition, OffsetAndMetadata> getCurrentOffset()
	{
		return currentOffset;
	}
	
	@Override
	public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
		
		System.out.println("Following Partition Revoked.......");
		
		for(TopicPartition partition : partitions)
		{
			System.out.println(partition.partition()+", ");
		}
		
		System.out.println("Following Partition Commited.....");
		
		for(TopicPartition partition : currentOffset.keySet())
		{
			System.out.println(partition.partition());
		}
		
		consumer.commitSync();
		currentOffset.clear();
	}

	@Override
	public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

		System.out.println("Following Partition Assigned.......");
		
		for(TopicPartition partition : partitions)
		{
			System.out.println(partition.partition()+", ");
		}
		
	}

}
