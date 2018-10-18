package org.projectspark.com.config;

import java.util.HashMap;
import java.util.Map;

public class Config {

	private static Map<String, Object> params;

	public static final String INPUT_PATH = "input.path";
	public static final String FILE_FORMAT = "file.format";

	public static final String OUTPUT_PATH = "output.path";
	public static final String QUERY = "hive.quey";
	public static final String OUTPUT_HIVE = "output.quey";
	public static final String QUERY_HBASE = "hbase.quey";
	

	public Config() {

		if (params == null)
			params = new HashMap<>();

	}

	public void setProperty(String key, Object o) {
		params.put(key, o);
	}

	public Object getEntry(String Key) {
		return params.get(Key);
	}

}
