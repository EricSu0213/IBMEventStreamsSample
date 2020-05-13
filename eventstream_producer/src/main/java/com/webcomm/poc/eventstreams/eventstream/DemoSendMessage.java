package com.webcomm.poc.eventstreams.eventstream;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.RecordMetadata;

import com.webcomm.poc.eventstrams.model.Producer;

public class DemoSendMessage {

	public static void main(String[] args) throws InterruptedException, ExecutionException, IOException {
		String customContent = "hello7";
//		String customContent = new String(Files.readAllBytes(Paths.get("C:\\tmp\\test.txt")));

		Producer producer = new Producer();
		RecordMetadata recordMetadata = producer.produce(customContent);

		System.out.println("topic: " + recordMetadata.topic());
		System.out.println("partition: " + recordMetadata.partition());
		System.out.println("offset: " + recordMetadata.offset());
		System.out.println("timestamp: " + recordMetadata.timestamp());
		System.out.println("Send Susccess");
		System.out.println("====================");
	}
}
