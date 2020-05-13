package com.webcomm.poc.eventstrams.model;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.webcomm.poc.eventstreams.util.ConfigReader;

public class Consumer {

	private final String topic = ConfigReader.getInstance().getProperty("topic");;
	private KafkaConsumer<String, String> kafkaConsumer;
    private final long POLL_DURATION = 1000;
    private final String consumerGroupId = "ericCG1";
	private final String USERNAME = "token";
	private final String API_KEY = ConfigReader.getInstance().getProperty("api_key");
	private final String bootstrap_servers = ConfigReader.getInstance().getProperty("bootstrap_servers");
	private final String certificates = ConfigReader.getInstance().getProperty("certificates");

	public Consumer() throws FileNotFoundException, IOException {
		try {
			kafkaConsumer = createConsumer();
		} catch (KafkaException e) {
			System.out.println(e.getMessage());
		}
        kafkaConsumer.subscribe(Arrays.asList(topic));
	}

	private KafkaConsumer<String, String> createConsumer() {
		Properties properties = new Properties();
		properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers);
		properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		properties.put(SslConfigs.SSL_PROTOCOL_CONFIG, "TLSv1.2");
		properties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, certificates);
		properties.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "password");
		properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
		String saslJaasConfig = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
				+ USERNAME + "\" password=" + API_KEY + ";";
		properties.put(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);

		KafkaConsumer<String, String> kafkaConsumer = null;

		try {
			kafkaConsumer = new KafkaConsumer<String, String>(properties);
		} catch (KafkaException kafkaError) {
			throw kafkaError;
		}
		
		return kafkaConsumer;
	}

    public ConsumerRecords<String, String> consume() {
        ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(POLL_DURATION));
        return records;
    }
    
    public void shutdown() {
        kafkaConsumer.close();
    }
}
