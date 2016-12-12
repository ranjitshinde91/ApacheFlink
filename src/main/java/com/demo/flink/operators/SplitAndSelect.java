package com.demo.flink.operators;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SplitAndSelect {

	public static void main(String[] args) throws Exception {
		
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
	     env.setParallelism(1);
	     
	     ArrayList<String> list = new ArrayList<String>();
	     list.add("Flink1");
	     list.add("Kafka");
	     list.add("Cassandra");
	     list.add("ElasticSearch");
	     
	     DataStreamSource<String> stream = env.fromCollection(list);
	     
		SplitStream<String> split = stream.split(new OutputSelector<String>() {
		    @Override
		    public Iterable<String> select(String value) {
		        List<String> output = new ArrayList<String>();
		        if (value.length() %2 ==0) {
		            output.add("even");
		        }
		        else {
		            output.add("odd");
		        }
		        return output;
		    }
		});
		
		DataStream<String> even = split.select("even");
		DataStream<String> odd = split.select("odd");
		DataStream<String> all = split.select("even","odd");
		
		even.print();
		odd.print();
		all.print();
		
		DataStream<String> even2 = split.select("even2");
		even2.print();
	
		env.execute();
	}
}
