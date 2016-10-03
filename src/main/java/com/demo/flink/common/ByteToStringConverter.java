package com.demo.flink.common;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

public  class ByteToStringConverter extends RichMapFunction<byte[], String>{
	int subtaskIndex = 5;
	
	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
	}
	
	@Override
	public String map(byte[] value) throws Exception {
		String message  = new String(value);
		System.out.println("message "+message+ " processed by subtaskIndex "+subtaskIndex);
		return message;
	}
	
}
