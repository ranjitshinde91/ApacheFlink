package com.demo.flink.state;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;

public class MessageFormatter extends RichMapFunction<String, String>{
	private transient ValueState<String> previousMessage;
	
	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		ValueStateDescriptor<String> descriptor =
                new ValueStateDescriptor<>(
                        "average", // the state name
                        TypeInformation.of(new TypeHint<String>() {}), // type information
                        new String("")); // default value of the state, if nothing was set
        previousMessage = getRuntimeContext().getState(descriptor);
	}
	@Override
	public void close() throws Exception {
		super.close();
	}
	
	
	@Override
	public String map(String value) throws Exception {
		System.out.println("Previous message is "+previousMessage.value() + " current message is "+value);
		previousMessage.update(value.toUpperCase());
		return previousMessage.value()+value.toUpperCase();
	}
}
