package com.demo.flink.sinks;


import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import com.demo.flink.common.MyDeserializationSchema;
import com.demo.flink.common.Sink;

public class KafkaSink {

	public static <T> void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(1000);
        
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "group");
        properties.setProperty("auto.offset.reset", "earliest"); 
        
        MyDeserializationSchema myDeserializationSchema = new MyDeserializationSchema();
        DataStreamSource<byte[]> stream = (DataStreamSource<byte[]>) env.addSource(new FlinkKafkaConsumer09<>("test-topic", (DeserializationSchema<T>)myDeserializationSchema, properties));
        
        SingleOutputStreamOperator<String> stringStream = stream.map(new MapFunction<byte[], String>() {

			@Override
			public String map(byte[] value) throws Exception {
				return new String(value);
			}
		});
        stringStream.addSink(new FlinkKafkaProducer09<>("localhost:9092","test-topic-output2",new SimpleStringSchema()));
        env.execute("Flink Kafka Sink tester");
	}
}
