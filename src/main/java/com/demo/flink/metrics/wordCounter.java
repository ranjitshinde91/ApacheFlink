package com.demo.flink.metrics;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;

public class wordCounter extends RichMapFunction<String, Tuple2<String, Integer>> {
	 private Counter counter;

	@Override
	  public void open(Configuration config) {
	    this.counter = getRuntimeContext()
	      .getMetricGroup()
	      .counter("myCounter");
	  }

	
	@Override
	public Tuple2<String, Integer> map(String value) throws Exception {
		this.counter.inc();
		return new Tuple2<String, Integer>(value, value.length());
	}
}
