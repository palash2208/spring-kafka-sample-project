package com.pal.learn.kafka.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import model.KafkaMessage;

@RestController
@RequestMapping("/api/kafka")
public class KafkaController {
	
	@Value("${spring.kafka.properties.bootstrap.servers}")
	private String kafkaServers;
	
	private KafkaTemplate<String, KafkaMessage> kafkaTemplate;
	
	@Autowired
	public KafkaController(KafkaTemplate<String, KafkaMessage> kafkaTemplate) {
		this.kafkaTemplate = kafkaTemplate;
	}

	@GetMapping("/produce")
	public String testing(@RequestParam String message) {
		
		KafkaMessage kafkaMessage = new KafkaMessage();
		kafkaMessage.setMessage(message);
		
		kafkaTemplate.send("sample", "comments", kafkaMessage);
		
		return "Sent kafka message - on sample - on server " + kafkaServers;
	}
	
}
