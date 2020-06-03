package com.nineleaps.kafka.custom;

import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class SupplierDeserializer implements Deserializer<Supplier> {

	private String encoding = "UTF8";

	@Override
	public Supplier deserialize(String topic, byte[] data) {
		
		try
		{
			if(data == null)
			{
				System.out.println("Null recieved at deserialize");
				return null;
			}
			
			ByteBuffer byteBuffer = ByteBuffer.wrap(data);
			int supplierId = byteBuffer.getInt();
			
			int sizeOfName = byteBuffer.getInt();
			
			byte[] nameByte = new byte[sizeOfName];
			byteBuffer.get(nameByte);
			
			String deserializedName = new String(nameByte, encoding);
			
			int sizeOfDate = byteBuffer.getInt();
			byte[] dateByte = new byte[sizeOfDate];
			byteBuffer.get(dateByte);
			
			String dateString = new String(dateByte, encoding);
			
			DateFormat df = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");
			
			return new Supplier(supplierId, deserializedName, df.parse(dateString));
		}
		catch (Exception e) {
			throw new SerializationException("Error when deserializing Supplier to byte[]");
		}
	}
}
