package com.gslab.flink.operator_chaining;

import java.util.ArrayList;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Map {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        
        ArrayList<String> nameList = new ArrayList<String>();
        nameList.add("Ranjit");
        nameList.add("Shinde");
        nameList.add("Apache");
        nameList.add("Flink");
        nameList.add("Drill");
        nameList.add("Kafka");

        DataStream<Tuple2<String, Integer>> dataStream = env.fromCollection(nameList)
                .map(new wordCounter());
        dataStream.print();
        env.execute("String length calculator");
    }

    public static class wordCounter extends RichMapFunction<String, Tuple2<String, Integer>> {
		@Override
		public Tuple2<String, Integer> map(String value) throws Exception {
			return new Tuple2<String, Integer>(value, value.length());
		}
    }

}