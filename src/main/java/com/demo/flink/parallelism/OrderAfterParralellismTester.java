package com.demo.flink.parallelism;


import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;

import com.demo.flink.common.ByteToStringConverter;
import com.demo.flink.common.MyDeserializationSchema;

public class OrderAfterParralellismTester {

	public static <T> void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(1000);
        env.disableOperatorChaining();
        
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "orderParralellism4");
        properties.setProperty("auto.offset.reset", "earliest"); 
        
        MyDeserializationSchema myDeserializationSchema = new MyDeserializationSchema();
        DataStreamSource<byte[]> stream = (DataStreamSource<byte[]>) env.addSource(new FlinkKafkaConsumer09<>("my-topic", (DeserializationSchema<T>)myDeserializationSchema, properties));
        stream.setParallelism(1);
        
        SingleOutputStreamOperator<String> stringStream = stream.map(new ByteToStringConverter()).setParallelism(3);
        SingleOutputStreamOperator<String> outputStream = stringStream.map(new MapFunction<String, String>() {
			@Override
			public String map(String value) throws Exception {
				return value;
			}
		}).setParallelism(1);
        
        outputStream.print();
        env.execute("Order tester");
	}
	
}


