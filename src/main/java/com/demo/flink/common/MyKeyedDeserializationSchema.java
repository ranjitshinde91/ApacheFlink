package com.demo.flink.common;
import java.io.IOException;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;

public class MyKeyedDeserializationSchema implements KeyedDeserializationSchema<Tuple2<byte[],Integer>>{

	@Override
	public TypeInformation<Tuple2<byte[],Integer>> getProducedType() {
		return TypeInfoParser.parse("Tuple2<byte[],Integer>");
	}

	@Override
	public Tuple2<byte[], Integer> deserialize(byte[] messageKey,byte[] message, String topic, int partition, long offset)throws IOException {
		Tuple2 tuple2 = new Tuple2();
		tuple2.f0 = message;
		tuple2.f1 = partition;
		return tuple2;
	}
	@Override
	public boolean isEndOfStream(Tuple2<byte[], Integer> nextElement) {
		return false;
	}

}
