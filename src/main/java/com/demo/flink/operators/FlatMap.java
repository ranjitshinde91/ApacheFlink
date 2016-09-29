package com.demo.flink.operators;

import java.util.ArrayList;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlatMap {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        
        ArrayList<String> list = new ArrayList<String>();
        list.add("Flink");
        list.add("Kafka");
        list.add("Cassandra");
        list.add("ElasticSearch");
        
        
        DataStreamSource<String> stream = env.fromCollection(list);

        DataStream<Tuple2<String, Integer>> dataStream = stream.flatMap(new wordCounter()).name("word count FlatMap");
        dataStream.print();
        env.execute("String length calculator");
    }

    public static class wordCounter extends RichFlatMapFunction<String, Tuple2<String, Integer>> {
    	
    	@Override
		public void close() throws Exception {
    		System.out.println("close() called "+Thread.currentThread().getName());
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			System.out.println("open() called "+Thread.currentThread().getName());
		}

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> collector)throws Exception {
			collector.collect(new Tuple2<String, Integer>(value, value.length()));
			
		}
    }

}