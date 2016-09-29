package com.demo.flink.library;
import java.io.IOException;
import java.util.Map;

import org.apache.flink.streaming.util.serialization.JSONDeserializationSchema;
import org.apache.sling.commons.json.JSONException;
import org.apache.sling.commons.json.JSONObject;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class jacksonObjectMapper extends JSONDeserializationSchema {

	public static void main(String[] args) throws JSONException, JsonParseException, JsonMappingException, IOException {

		ObjectMapper mapper = new ObjectMapper();
		JSONObject json = new JSONObject();
		json.put("name", "Ranjit");
		json.put("age", 25);

		JSONObject json2 = new JSONObject();
		json2.put("country", "India");

		String message1 = json.toString();
		String message2 = json2.toString();

		ObjectNode objectNode1 = mapper.readValue(message1, ObjectNode.class);
		ObjectNode objectNode2 = mapper.readValue(message2, ObjectNode.class);

		System.out.println(objectNode1.get("name").asText());
		System.out.println(objectNode1.get("age").asInt());

		System.out.println(objectNode2.get("country").asText());

		System.out.println(objectNode2.get("country").isTextual());
		System.out.println(objectNode2.get("country").isInt());
		
		//POJO conversion
		User user =  mapper.convertValue(objectNode1, User.class);
		System.out.println("POJO: "+ user.getName()+" -"+user.getAge());
		
		objectNode1.put("objectNodeField", true);
		
		System.out.println(objectNode1.get("objectNodeField").asBoolean());
		
		JSONObject json3 = new JSONObject(objectNode1.toString());
		System.out.println(json3);
		
		Map<String, Object> result = mapper.convertValue(objectNode1, Map.class);
		System.out.println(result.get("name"));
		
		
	}
}
