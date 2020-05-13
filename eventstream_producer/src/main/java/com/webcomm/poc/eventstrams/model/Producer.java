package com.webcomm.poc.eventstrams.model;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.ConnectException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;

import com.webcomm.poc.eventstreams.util.ConfigReader;

public class Producer {

	private final String topic = ConfigReader.getInstance().getProperty("topic");;
	private KafkaProducer<String, String> kafkaProducer;
	private final String USERNAME = "token";
	private final String API_KEY = ConfigReader.getInstance().getProperty("api_key");
	private final String bootstrap_servers = ConfigReader.getInstance().getProperty("bootstrap_servers");
	private final String certificates = ConfigReader.getInstance().getProperty("certificates");

	public Producer() throws FileNotFoundException, IOException {
		try {
			kafkaProducer = createProducer();
		} catch (KafkaException e) {
			System.out.println(e.getMessage());
		}
	}

	private KafkaProducer<String, String> createProducer() {
		Properties properties = new Properties();
		properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers);
		properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
//        properties.put(CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG, 10000);
//        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 7000);
//        properties.put(ProducerConfig.RETRIES_CONFIG, 0);
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		properties.put(SslConfigs.SSL_PROTOCOL_CONFIG, "TLSv1.2");
		properties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, certificates);
		properties.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "password");
		properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
		String saslJaasConfig = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
				+ USERNAME + "\" password=" + API_KEY + ";";
		properties.put(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);

		KafkaProducer<String, String> kafkaProducer = null;

		try {
			kafkaProducer = new KafkaProducer<String, String>(properties);
		} catch (KafkaException kafkaError) {
			throw kafkaError;
		}
		
		return kafkaProducer;
	}

	public RecordMetadata produce(String message) throws InterruptedException, ExecutionException, ConnectException {
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, "ericPartition4", message);
		RecordMetadata recordMetadata = kafkaProducer.send(record).get();
		
		return recordMetadata;
	}

	public void shutdown() {
		kafkaProducer.flush();
		kafkaProducer.close();
	}
}
