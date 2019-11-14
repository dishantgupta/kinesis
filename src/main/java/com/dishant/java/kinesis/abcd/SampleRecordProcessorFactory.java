package com.dishant.java.kinesis.abcd;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;

@Service
public class SampleRecordProcessorFactory implements ShardRecordProcessorFactory {

	@Autowired
	private SampleRecordProcessor sampleRecordProcessor;

		public ShardRecordProcessor shardRecordProcessor() {
			return sampleRecordProcessor;
		}
	}
