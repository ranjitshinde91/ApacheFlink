package com.demo.flink.utility;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.demo.flink.operators.KeyBy;

public class FlinkKafkaPartitionAssigner {
	
	private static Logger LOGGER = LoggerFactory.getLogger(FlinkKafkaPartitionAssigner.class);
	public static void main(String[] args) {
		ArrayList<KafkaTopicPartition> allPartitions = new ArrayList<KafkaTopicPartition>();
		allPartitions.add(new KafkaTopicPartition("my-topic",0));
		allPartitions.add(new KafkaTopicPartition("my-topic",1));
		allPartitions.add(new KafkaTopicPartition("my-topic",2));
		
		LOGGER.info("3 kafka topics 2 consumers:");
		LOGGER.info("Consumer 0: "+assignPartitions( allPartitions, 2, 0).toString());
		LOGGER.info("Consumer 1: "+assignPartitions(allPartitions, 2, 1).toString());
		
		
		
		LOGGER.info("3 kafka topics 3 consumers:");
		LOGGER.info("Consumer 0: "+assignPartitions( allPartitions, 3, 0).toString());
		LOGGER.info("Consumer 1: "+assignPartitions(allPartitions, 3, 1).toString());
		LOGGER.info("Consumer 2: "+assignPartitions(allPartitions, 3, 2).toString());
		
		LOGGER.info("3 kafka topics 4 consumers:");
		
		LOGGER.info("Consumer 0: "+assignPartitions(allPartitions, 4, 0).toString());
		LOGGER.info("Consumer 1: "+assignPartitions(allPartitions, 4, 1).toString());
		LOGGER.info("Consumer 2: "+assignPartitions(allPartitions, 4, 2).toString());
		LOGGER.info("Consumer 3: "+assignPartitions(allPartitions, 4, 3).toString());
	}

	public static List<KafkaTopicPartition> assignPartitions(List<KafkaTopicPartition> allPartitions, int numConsumers, int consumerIndex) {
		final List<KafkaTopicPartition> thisSubtaskPartitions = new ArrayList<>(
				allPartitions.size() / numConsumers + 1);

		for (int i = 0; i < allPartitions.size(); i++) {
			if (i % numConsumers == consumerIndex) {
				thisSubtaskPartitions.add(allPartitions.get(i));
			}
		}

		return thisSubtaskPartitions;
	}

}
