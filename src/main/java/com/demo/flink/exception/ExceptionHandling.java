package com.demo.flink.exception;

import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;

import com.demo.flink.common.MyDeserializationSchema;
import com.demo.flink.common.MyException;
import com.demo.flink.common.Sink;

public class ExceptionHandling {

	public static <T> void main(String[] args) throws Exception  {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(1000);
        
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "exception");
        properties.setProperty("auto.offset.reset", "earliest"); 
        
        MyDeserializationSchema myDeserializationSchema = new MyDeserializationSchema();
        DataStreamSource<byte[]> stream = (DataStreamSource<byte[]>) env.addSource(new FlinkKafkaConsumer09<>("my-topic", (DeserializationSchema<T>)myDeserializationSchema, properties));
        
        SingleOutputStreamOperator<String> stringStream = stream.map(new MapFunction<byte[], String>() {

			@Override
			public String map(byte[] value){
				try{
					return new String(value);
				}
				catch(Exception e){
					System.out.println(e.getMessage());
					return "";
				}
			}
		});
        stringStream.addSink(new Sink());
        
        env.execute("Exception Handling");
	}
}
