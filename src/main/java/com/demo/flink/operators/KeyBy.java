package com.demo.flink.operators;

import java.util.ArrayList;
import java.util.Random;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KeyBy {

	private static Logger LOGGER = LoggerFactory.getLogger(KeyBy.class);
	public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        ArrayList<String> list = new ArrayList<String>();
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

        SingleOutputStreamOperator<Tuple2<String,Integer>> keyedDataStream = stream.keyBy(new keyByLength())
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
    	private String state;
    	private transient ValueState<String> flinkState;
    	
    	@Override
		public void close() throws Exception {
    		LOGGER.info("Close() called "+getRuntimeContext().getIndexOfThisSubtask());
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			LOGGER.info("Open() called "+getRuntimeContext().getIndexOfThisSubtask());
			Random rand = new Random();
			int  random = rand.nextInt(50) + 1;
			
			state= "state Present";
			ValueStateDescriptor<String> descriptor =
	                new ValueStateDescriptor<>(
	                        "average", // the state name
	                        TypeInformation.of(new TypeHint<String>() {}), // type information
	                        new String(Integer.toString(random))); // default value of the state, if nothing was set
	       flinkState = getRuntimeContext().getState(descriptor);
		}

		@Override
		public Tuple2<String, Integer> map(String value) throws Exception {
			LOGGER.info("instance variable for: "+value+" - "+state);
			LOGGER.info("state variable before for "+value+" - "+flinkState.value());
			flinkState.update(value);
			LOGGER.info("state variable after for "+value+" - "+flinkState.value());
			
			LOGGER.info("message "+value+" came to  - "+getRuntimeContext().getIndexOfThisSubtask());
			return new Tuple2<String, Integer>(value, getRuntimeContext().getIndexOfThisSubtask());
		}
    }
}
