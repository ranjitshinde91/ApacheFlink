package com.demo.flink.operators;

import java.util.ArrayList;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.demo.flink.source.SequenceGenerator;

/**
 * output
 *  Cassandra
 *  ElasticSearch
 */
public class Filter {

	private static Logger LOGGER = LoggerFactory.getLogger(SequenceGenerator.class);
	public static void main(String[] args) throws Exception {
		 StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
	     env.setParallelism(1);
	     
	     ArrayList<String> list = new ArrayList<String>();
	     list.add("Flink");
	     list.add("Kafka");
	     list.add("Cassandra");
	     list.add("ElasticSearch");
	     
	     
	     DataStreamSource<String> stream = env.fromCollection(list);
	
	     DataStream<String> dataStream = stream.filter(new wordLengthFilter()).name("length Filter");
	     dataStream.print();
	     env.execute("String length Filter ");
	 }

	 public static class wordLengthFilter extends RichFilterFunction<String> {
	 	
	 	@Override
			public void close() throws Exception {
	 		LOGGER.info("close() called "+Thread.currentThread().getName());
			}
	
			@Override
			public void open(Configuration parameters) throws Exception {
				LOGGER.info("open() called "+Thread.currentThread().getName());
			}
	
			@Override
			public boolean filter(String value) throws Exception {
				return value.length()>5;
			}
	}
}

