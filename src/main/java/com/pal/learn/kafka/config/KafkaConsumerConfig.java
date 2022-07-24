package com.pal.learn.kafka.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import model.KafkaMessage;


@Configuration
public class KafkaConsumerConfig {

	@Value("${spring.kafka.properties.bootstrap.servers}")
	private String kafkaServers;
	
	@Value("${CONFLUENT_API_KEY}")
	private String username;
	
	@Value("${CONFLUENT_API_PASSWORD}")
	private String password;
	
	public Map<String, Object> consumerConfig() {
		Map<String, Object> properties = new HashMap<>();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
		
//		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
				
		properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
		properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
	    properties.put(SaslConfigs.SASL_JAAS_CONFIG, String.format(
	            "%s required username=\"%s\" " + "password=\"%s\";", PlainLoginModule.class.getName(), username, password
	    ));
	    
	    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
	    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		return properties;
	}
	
	@Bean
	public ConsumerFactory<String, KafkaMessage> consumerFactory() {
		JsonDeserializer<KafkaMessage> jsonDeserializer = new JsonDeserializer<>();
        jsonDeserializer.addTrustedPackages("*");
        return new DefaultKafkaConsumerFactory<>(
                consumerConfig(),
                new StringDeserializer(),
                jsonDeserializer
        );

	}
	
	@Bean
    public ConcurrentKafkaListenerContainerFactory<String, KafkaMessage>
                        kafkaListenerContainerFactory(ConsumerFactory<String, KafkaMessage> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, KafkaMessage> factory =
                                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        return factory;
    }
	
}
