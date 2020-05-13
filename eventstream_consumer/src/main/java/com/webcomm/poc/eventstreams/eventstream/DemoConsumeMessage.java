package com.webcomm.poc.eventstreams.eventstream;

import java.io.IOException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import com.webcomm.poc.eventstrams.model.Consumer;

public class DemoConsumeMessage {

	public static void main(String[] args) throws IOException {
		Consumer consumer = new Consumer();

		while (true) {
			ConsumerRecords<String, String> records = consumer.consume();
			for (ConsumerRecord<String, String> record : records) {
				System.out.println("topic: " + record.topic());
				System.out.println("partition: " + record.partition());
				System.out.println("offset: " + record.offset());
				System.out.println("key: " + record.key());
				System.out.println("value: " + record.value());
				System.out.println("timestamp: " + record.timestamp());
				System.out.println("====================");
//	    		Path path = Paths.get("C:\\tmp\\save.txt");
//	    		Files.write(path, record.value().getBytes());
			}
		}

//        System.out.println("consume end");
//        kafkaConsumer.close();
	}
}
