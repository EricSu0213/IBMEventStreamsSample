package com.webcomm.poc.eventstreams.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class ConfigReader {

	private static ConfigReader instance = null;

	private static String configFilePath = "./src/main/resources/config.properties";

	public static ConfigReader getInstance() {
		if (instance == null) {
			instance = new ConfigReader();
		}
		return instance;
	}

	public String getProperty(String key) throws FileNotFoundException, IOException {
		Properties properties = new Properties();
		FileInputStream io = new FileInputStream(configFilePath);
		properties.load(io);
        String value = properties.getProperty(key);
		io.close();
        
		return value;
	}
}
