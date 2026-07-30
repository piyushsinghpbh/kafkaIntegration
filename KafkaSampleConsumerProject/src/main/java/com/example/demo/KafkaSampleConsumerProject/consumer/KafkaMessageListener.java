package com.example.demo.KafkaSampleConsumerProject.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.stereotype.Service;

import com.example.demo.KafkaSampleConsumerProject.dto.Customer;

@Service
public class KafkaMessageListener {

	Logger log = LoggerFactory.getLogger(KafkaMessageListener.class);

	@KafkaListener(topics = "test-topic1", groupId = "jt-group")
	public void consumeEvents(Customer customer) {
		log.info("consumer consume the events {} ", customer.toString());
	}
	
	 @KafkaListener(topics = "test-topic",groupId = "jt-group")
	 public void consume1(String message) {
		 log.info("consumer1 consume the message {} ", message);
	 }
	 @KafkaListener(topics = "test-topic",groupId = "jt-group")
	 public void consume2(String message) {
		 log.info("consumer2 consume the message {} ", message);
	 }
	 @KafkaListener(topics = "test-topic",groupId = "jt-group")
	 public void consume3(String message) {
		 log.info("consumer3 consume the message {} ", message);
	 }
	 @KafkaListener(topics = "test-topic",groupId = "jt-group")
	 public void consume4(String message) {
		 log.info("consumer4 consume the message {} ", message);
	 }
	 @KafkaListener(topics = "test-topic",groupId = "jt-group")
	 public void consume5(String message) {
		 log.info("consumer5 consume the message {} ", message);
	 }
}
