package com.kafka.demo.KafkaSampleProducerProject.service;

import java.util.concurrent.CompletableFuture;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.kafka.demo.KafkaSampleProducerProject.dto.Customer;

import org.springframework.kafka.support.SendResult;


@Service
public class DefaultKafkaMessagePublisher {
	
	
	@Autowired
	private KafkaTemplate<String, Object> template;
	
	
	public void sendMessageToTopic(String message) {
		try {
		CompletableFuture<SendResult<String, Object>> future = template.send("test-topic", message);
		future.whenComplete((result, exc) -> {
			if (exc == null) {
				System.out.println(
						"Sent message=[" + message + "] with offset =[" + result.getRecordMetadata().offset() + "]");
			} else {
				System.out.println("Unable to Send message=[" + message + "] due to:" + exc.getMessage());
			}
		});
		} catch(Exception ex) {
			System.out.println("Unable to Send message=[" + message + "] due to:" + ex.getMessage());
			
		}
		
	}
	
	
	  public void sendEventsToTopic(Customer customer) {
			CompletableFuture<org.springframework.kafka.support.SendResult<String, Object>> future = template
					.send("test-topic", customer);
			future.whenComplete((result, ex) -> {
				if (ex == null) {
					System.out.println(
							"Sent message=[" + customer.toString() + "] with offset =[" + result.getRecordMetadata().offset() + "]");
				} else {
					System.out.println("Unable to Send message=[" + customer.toString() + "] due to:" + ex.getMessage());
				}
			});
		}
	 
	
	

}
