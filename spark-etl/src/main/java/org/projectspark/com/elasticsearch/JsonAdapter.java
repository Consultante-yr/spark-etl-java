package org.projectspark.com.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonAdapter {

	public static ObjectMapper mapper = new ObjectMapper();


	public static <T> String Serialize(T obj) {
		String jsonString = "";

		try {
			jsonString = mapper.writeValueAsString(obj);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return jsonString;
	}


	public static <T> T Deserialize(String jsonString, Class<T> c) {
		T obj = null;
		try {
			obj = mapper.readValue(jsonString, c);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return obj;
	}
	
	
}

