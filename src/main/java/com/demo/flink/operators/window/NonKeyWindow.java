package com.demo.flink.operators.window;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;

public class NonKeyWindow {
	
	public static void main(String[] args) {

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

        
        DataStreamSource<String> stream = env.fromCollection(list);
        
	}

}
