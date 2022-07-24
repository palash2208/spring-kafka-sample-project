package com.pal.learn.kafka.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.converter.JsonMessageConverter;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.support.serializer.JsonSerializer;

import model.KafkaMessage;

@Configuration
public class KafkaProducerConfig {
	
	@Value("${spring.kafka.properties.bootstrap.servers}")
	private String kafkaServers;
	
	@Value("${CONFLUENT_API_KEY}")
	private String username;
	
	@Value("${CONFLUENT_API_PASSWORD}")
	private String password;
	
	public Map<String, Object> producerConfig() {
		Map<String, Object> properties = new HashMap<>();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
		/* properties.put(ProducerConfig.ACKS_CONFIG, "1"); */
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
				
		properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
		properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
	    properties.put(SaslConfigs.SASL_JAAS_CONFIG, String.format(
	            "%s required username=\"%s\" " + "password=\"%s\";", PlainLoginModule.class.getName(), username, password
	    ));
		return properties;
	}
	
	@Bean
	public ProducerFactory<String, KafkaMessage> producerFactory() {
		return new DefaultKafkaProducerFactory<>(producerConfig());
	}
	
	@Bean
	public KafkaTemplate<String, KafkaMessage> kafkaTemplate(
			ProducerFactory<String, KafkaMessage> producerFactory) {
		return new KafkaTemplate<>(producerFactory);
	}
	
	@Bean
    public RecordMessageConverter converter() {
        return new JsonMessageConverter();
    }
	
}
