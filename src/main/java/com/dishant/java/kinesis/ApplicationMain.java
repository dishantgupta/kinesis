package com.dishant.java.kinesis;

import com.dishant.java.kinesis.abcd.ApplicationConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.KinesisClientUtil;

@SpringBootApplication
public class ApplicationMain {

	@Autowired
	private ApplicationConfiguration applicationConfiguration;

	public static void main(String[] args) throws InterruptedException {
		SpringApplication.run(ApplicationMain.class, args);
	}

	@Bean
	public AwsCredentialsProvider awsCredentialsProvider() {
		AwsCredentialsProvider awsCredentialsProvider = new AwsCredentialsProvider() {
			@Override
			public AwsCredentials resolveCredentials() {
				AwsCredentials awsCredentials = new AwsCredentials() {
					@Override
					public String accessKeyId() {
						return applicationConfiguration.getAwsAccessKey();
					}

					@Override
					public String secretAccessKey() {
						return applicationConfiguration.getAwsSecretKey();
					}
				};
				return awsCredentials;
			}
		};
		return awsCredentialsProvider;
	}

	@Bean
	public KinesisAsyncClient kinesisAsyncClient() {
		return KinesisClientUtil.createKinesisAsyncClient(
				KinesisAsyncClient.builder()
						.credentialsProvider(awsCredentialsProvider())
						.region(Region.of(applicationConfiguration.getAwsRegionCode())));
	}

}
