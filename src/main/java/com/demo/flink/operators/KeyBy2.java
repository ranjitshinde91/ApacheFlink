package com.demo.flink.operators;

import java.util.ArrayList;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.demo.flink.operators.KeyBy.keyByLength;
import com.demo.flink.operators.KeyBy.wordCounter;

public class KeyBy2 {

	public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        ArrayList<Integer> list = new ArrayList<Integer>();
        list.add(1);
        list.add(2);
        list.add(1);
        
        DataStreamSource<Integer> stream = env.fromCollection(list);

        KeyedStream<Integer,Integer> keyedDataStream = stream.keyBy(new KeySelector<Integer, Integer>(){

			@Override
			public Integer getKey(Integer value) throws Exception {
				return value;
			}
		    });
        
        
        keyedDataStream.map(new RichMapFunction<Integer, Integer>() {

			@Override
			public Integer map( Integer value)throws Exception {
				System.out.println("record "+value + " -"+getRuntimeContext().getIndexOfThisSubtask());
				return value;
			}
		   })
		   .name("word count map")
		   .setParallelism(4);
        env.execute("String length calculator");
    }
}
