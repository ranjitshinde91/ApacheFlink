package com.demo.flink.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SequenceGenerator {

	private static Logger LOGGER = LoggerFactory.getLogger(SequenceGenerator.class);
	public static void main(String[] args) throws Exception {
		
		LOGGER.info("execution started");
		
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		
		DataStreamSource<Long> stream = env.generateSequence(1, 10);
		stream.print();
		
		env.execute();
		
		LOGGER.info("execution complete");
		
	}
}
