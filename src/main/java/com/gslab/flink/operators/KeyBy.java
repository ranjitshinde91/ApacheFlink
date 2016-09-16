package com.gslab.flink.operators;

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
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
7> (Flink,5)
7> (Kafka,5)
4> (Dassandra,9)
4> (Cassandra,9)

Data with same key always goes to same sink, i.e it processes this by sequencally for key
it could  impact application performance if used frequently


instance varibales will be shared even if you do keyBy, keyBy will share operators
state will be differrent for each key

**/
public class KeyBy {

	
	public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStateBackend(new FsStateBackend("file:///C://flink/"));
        
        ArrayList<String> list = new ArrayList<String>();
        list.add("Flink");
        list.add("Kafka");
        list.add("Cassandra");
        list.add("Dassandra");
        list.add("ElasticSearch");
        list.add("Drill");
        list.add("S3");
        
        
        DataStreamSource<String> stream = env.fromCollection(list);

        SingleOutputStreamOperator<Tuple2<String,Integer>> keyedDataStream = stream.keyBy(new keyByLength()).map(new wordCounter()).name("word count map");
        keyedDataStream.print();
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
    		System.out.println("Close() called "+Thread.currentThread().getName());
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			System.out.println("Open() called "+Thread.currentThread().getName());
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
			System.out.println("instance variable for: "+value+" - "+state);
			System.out.println("state variable before for "+value+" - "+flinkState.value());
			flinkState.update(value);
			System.out.println("state variable after for "+value+" - "+flinkState.value());
			return new Tuple2<String, Integer>(value, value.length());
		}
    }
}
