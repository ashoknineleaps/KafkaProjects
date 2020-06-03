package com.nineleaps.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class MyProducerCallback implements Callback {

	@Override
	public void onCompletion(RecordMetadata recordMetadata, Exception ex) {
		
		if(ex != null)
		{
			System.err.println("AsynchronousProducer failed with an exception.");
		}
		else
		{
			System.out.println("AsynchronousProducer call Success.");
		}
	}

}
