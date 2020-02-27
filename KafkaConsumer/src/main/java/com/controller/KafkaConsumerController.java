package com.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import com.response.Response;
import com.service.KafkaConsumerService;

@RestController
public class KafkaConsumerController {
	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerController.class);
	@Autowired
	KafkaConsumerService kafkaConsumerService;

	@GetMapping("/customer/subscribe")
	public ResponseEntity<Response> consumeCustomerJsonFromTopic() {
		return kafkaConsumerService.consumeCustomerFromTopic();
	}
}
