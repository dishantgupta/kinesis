package com.dishant.java.kinesis.abcd;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;

@Slf4j
@Service
public class Publisher {

	@Autowired
	private KinesisAsyncClient kinesisAsyncClient;

	public void publish(String streamName, Object message){
		PutRecordRequest request = PutRecordRequest.builder()
				.partitionKey("parition")
				.streamName(streamName)
				.data(SdkBytes.fromByteBuffer(ByteBuffer.wrap("message".getBytes())))
				.build();
		try {
			kinesisAsyncClient.putRecord(request).get();
		} catch (Exception e) {
			// wrap exception
		}
	}
}
