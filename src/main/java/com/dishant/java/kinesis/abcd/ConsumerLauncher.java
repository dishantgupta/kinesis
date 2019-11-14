package com.dishant.java.kinesis.abcd;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;
import software.amazon.kinesis.retrieval.polling.PollingConfig;

import java.util.UUID;

@Service
public class ConsumerLauncher {

	@Autowired
	private KinesisAsyncClient kinesisClient;

	public void launch(String streamName, Region region, ShardRecordProcessorFactory shardRecordProcessorFactory){
		DynamoDbAsyncClient dynamoClient = DynamoDbAsyncClient.builder().region(region).build();
		CloudWatchAsyncClient cloudWatchClient = CloudWatchAsyncClient.builder().region(region).build();
		ConfigsBuilder configsBuilder = new ConfigsBuilder(streamName, streamName, kinesisClient, dynamoClient, cloudWatchClient, UUID
				.randomUUID().toString(), shardRecordProcessorFactory);
		Scheduler scheduler = new Scheduler(
				configsBuilder.checkpointConfig(),
				configsBuilder.coordinatorConfig(),
				configsBuilder.leaseManagementConfig(),
				configsBuilder.lifecycleConfig(),
				configsBuilder.metricsConfig(),
				configsBuilder.processorConfig(),
				configsBuilder.retrievalConfig().retrievalSpecificConfig(new PollingConfig(streamName,
						kinesisClient))
		);
		Thread schedulerThread = new Thread(scheduler);
		schedulerThread.setDaemon(true);
		schedulerThread.start();
	}
}
