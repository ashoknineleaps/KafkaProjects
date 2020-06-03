package com.nineleaps.kafka.custom;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class SupplierSerializer implements Serializer<Supplier> {
	
	private String encoding = "UTF8";

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		Serializer.super.configure(configs, isKey);
	}
	@Override
	public byte[] serialize(String topic, Supplier data) {
		
		int sizeOfName;
		int sizeOfDate;
		byte[] serializedName;
		byte[] serializedDate;
		
		try
		{
			if(data == null)
			{
				return null;
			}
			
			serializedName = data.getSupplierName().getBytes(encoding);
			sizeOfName = serializedName.length;
			
			serializedDate = data.getSupplierStartDate().toString().getBytes(encoding);
			sizeOfDate = serializedDate.length;
			
			ByteBuffer byteBuffer = ByteBuffer.allocate(4 + 4 + sizeOfName + 4 + sizeOfDate);
			byteBuffer.putInt(data.getSupplierId());
			byteBuffer.putInt(sizeOfName);
			byteBuffer.put(serializedName);
			byteBuffer.putInt(sizeOfDate);
			byteBuffer.put(serializedDate);
			
			return byteBuffer.array();
		}
		catch (Exception e) {
			throw new SerializationException("Error when serializing Supplier to byte[]");
		}
	}

	@Override
	public void close() {
		Serializer.super.close();
	}
}
