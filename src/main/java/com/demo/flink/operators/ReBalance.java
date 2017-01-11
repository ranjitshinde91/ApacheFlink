package com.demo.flink.operators;

import java.util.LinkedList;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReBalance {

	private static Logger LOGGER = LoggerFactory.getLogger(KeyBy.class);
	public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        LinkedList<String> list = new LinkedList<String>();
        list.add("Flink");
        list.add("Kafka");
        list.add("Cassandra");
        list.add("Dassandra");
        list.add("ElasticSearch");
        list.add("Drill");
        list.add("S3");
        list.add("S4");
        list.add("1");
        list.add("3");
        
        
        DataStreamSource<String> stream = env.fromCollection(list);

        SingleOutputStreamOperator<Tuple2<String,Integer>> keyedDataStream = stream.rebalance()
        																		   .map(new wordCounter())
        																		   .name("word count map")
        																		   .setParallelism(4);
        env.execute("String length calculator");
    }

	
	public static class keyByLength implements KeySelector<String, Integer>{
		@Override
		public Integer getKey(String value) throws Exception {
			return value.length();
		}
	}
	
	public static class wordCounter extends RichMapFunction<String, Tuple2<String, Integer>> {
    	
		@Override
		public Tuple2<String, Integer> map(String value) throws Exception {
			LOGGER.info("message "+value+" came to  - "+getRuntimeContext().getIndexOfThisSubtask());
			return new Tuple2<String, Integer>(value, getRuntimeContext().getIndexOfThisSubtask());
		}
    }
}
