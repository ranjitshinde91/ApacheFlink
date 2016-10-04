package com.demo.flink.operators;

import java.util.ArrayList;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Union {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(8);
        
        ArrayList<String> list = new ArrayList<String>();
        list.add("Flink");
        list.add("Kafka");
        list.add("Cassandra");
        list.add("ElasticSearch");
        
        
        DataStreamSource<String> stream = env.fromCollection(list);

        DataStream<String> unionStream =  stream.union(stream);
        
        unionStream.print();
        env.execute("Union");
    }


}