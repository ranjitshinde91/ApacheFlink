package com.demo.flink.state;
import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;

import com.demo.flink.common.MyDeserializationSchema;
import com.demo.flink.common.Sink;

public class FlinkStateManager {

	public static <T> void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(1000);
        env.disableOperatorChaining();
        
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "group2");
        properties.setProperty("auto.offset.reset", "earliest"); 
        
        MyDeserializationSchema myDeserializationSchema = new MyDeserializationSchema();
        DataStreamSource<byte[]> stream = (DataStreamSource<byte[]>) env.addSource(new FlinkKafkaConsumer09<>("my-topic", (DeserializationSchema<T>)myDeserializationSchema, properties));
        
        SingleOutputStreamOperator<String> stringStream = stream.map(new MapFunction<byte[], String>() {

			@Override
			public String map(byte[] value) throws Exception {
				return new String(value);
			}
		});
        KeyedStream<String, String> keyedMessageStream = stringStream.keyBy(new KeySelector<String, String>() {

			@Override
			public String getKey(String value) throws Exception {
				return "key";
			}
		});
        SingleOutputStreamOperator<String> formattedMessageStream = keyedMessageStream.map(new MessageFormatter());
        formattedMessageStream.addSink(new Sink());
        env.execute("Flink state tester");
	}
}

