package com.pal.learn.kafka.controller;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import model.KafkaMessage;

@Controller()
@RequestMapping("/kafka")
public class ViewController {
	
	@Value("${spring.kafka.properties.bootstrap.servers}")
	private String kafkaServers;
	
	private KafkaTemplate<String, KafkaMessage> kafkaTemplate;
	
	List<String> messageList = new ArrayList<>();

	@Autowired
	public ViewController(KafkaTemplate<String, KafkaMessage> kafkaTemplate) {
		this.kafkaTemplate = kafkaTemplate;
	}
	
	@GetMapping("/produce")
	public String produce(Model model) {
		
		model.Model model1 = new model.Model();
		model.addAttribute("model1", model1);
		return "kafkacreate";
	}
	
	@PostMapping("/send")
	public String send(@ModelAttribute model.Model message, Model model) {
		
		KafkaMessage kafkaMessage = new KafkaMessage();
		kafkaMessage.setMessage(message.getMessage());
		kafkaTemplate.send("sample", "comments", kafkaMessage);
		
		return "redirect:/kafka/produce";
	}
	
	@GetMapping("/consume")
	public String consume(Model model) {
		model.addAttribute("messageList", messageList);
		return "kafka";
	}
	
	
	
	@KafkaListener(topics = "sample", groupId = "sample-groud-id-2", containerFactory = "kafkaListenerContainerFactory")
	public void consume(ConsumerRecord<String, KafkaMessage> consumerRecord) {
		String key = consumerRecord.key();
		KafkaMessage value = consumerRecord.value();
		System.out.println("value from group2 => " + key + value.getMessage());
		messageList.add(value.getMessage() + " - with " + key);
		
	}
}
