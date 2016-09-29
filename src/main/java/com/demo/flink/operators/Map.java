package com.demo.flink.operators;

import java.util.ArrayList;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Map {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(8);
        
        ArrayList<String> list = new ArrayList<String>();
        list.add("Flink");
        list.add("Kafka");
        list.add("Cassandra");
        list.add("ElasticSearch");
        
        
        DataStreamSource<String> stream = env.fromCollection(list);

        DataStream<Tuple2<String, Integer>> dataStream = stream.map(new wordCounter()).name("word count map");
        dataStream.print();
        env.execute("String length calculator");
    }

    public static class wordCounter extends RichMapFunction<String, Tuple2<String, Integer>> {
    	
    	@Override
		public void close() throws Exception {
    		System.out.println("close() called "+Thread.currentThread().getName());
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			System.out.println("open() called "+Thread.currentThread().getName());
		}

		@Override
		public Tuple2<String, Integer> map(String value) throws Exception {
			return new Tuple2<String, Integer>(value, value.length());
		}
    }

}