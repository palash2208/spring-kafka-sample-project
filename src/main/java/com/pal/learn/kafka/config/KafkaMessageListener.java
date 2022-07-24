package com.pal.learn.kafka.config;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import model.KafkaMessage;

@Component
public class KafkaMessageListener {

	@KafkaListener(topics = "sample", groupId = "sample-groud-id-1", containerFactory = "kafkaListenerContainerFactory")
	public void consume(ConsumerRecord<String, KafkaMessage> consumerRecord) {
		String key = consumerRecord.key();
		KafkaMessage value = consumerRecord.value();
		
		System.out.println("value from group 1 => " + key + value.getMessage());
		
	}
	
}
