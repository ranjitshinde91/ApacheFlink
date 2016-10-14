package com.demo.flink.connectors;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch2.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

public class ElasticSearch2X {
	public static void main(String[] args) throws Exception {
		
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        
        ArrayList<String> list = new ArrayList<String>();
        list.add("Flink");
        list.add("Kafka");
        list.add("Cassandra");
        list.add("ElasticSearch");
        
        
        DataStreamSource<String> inputStream = env.fromCollection(list);
		
		Map<String, String> config = new HashMap<>();
		// This instructs the sink to emit after every element, otherwise they would be buffered
		config.put("bulk.flush.max.actions", "1");
		config.put("cluster.name", "my-application");

		List<InetSocketAddress> transports = new ArrayList<>();
		transports.add(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9300));
		
		 ElasticsearchSinkFunction<String> elastciSearchFunction = new ElasticsearchSinkFunction<String>() {
			  public IndexRequest createIndexRequest(String element) {
			    Map<String, String> json = new HashMap<>();
			    json.put("data", element);

			    return Requests.indexRequest()
			            .index("my-index")
			            .type("my-type")
			            .source(json);
			  }

			  @Override
			  public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
			    indexer.add(createIndexRequest(element));
			  }
		 };
		 
		inputStream.addSink(new ElasticsearchSink(config, transports, elastciSearchFunction));
		env.execute();
	}
}
