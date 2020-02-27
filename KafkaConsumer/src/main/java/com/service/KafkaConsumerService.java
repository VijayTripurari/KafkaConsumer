package com.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import com.constants.CustomerConstants;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.model.AuditLog;
import com.model.Customer;
import com.model.ErrorLog;
import com.repository.AuditLogRepository;
import com.repository.CustomerRepository;
import com.repository.ErrorLogRepository;
import com.response.Response;

@Service
public class KafkaConsumerService {
	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerService.class);

	@Autowired
	Environment env;
	ResponseEntity<Response> responseEntity;
	@Autowired
	CustomerRepository customerRepository;
	@Autowired
	AuditLogRepository auditLogRepository;
	@Autowired
	ErrorLogRepository errorLogRepository;

	public ResponseEntity<Response> consumeCustomerFromTopic() {
		return responseEntity;
	}

	@KafkaListener(topics = CustomerConstants.TOPIC_NAME, groupId = CustomerConstants.KAFKA_GRP_ID, containerFactory = "kafkaListenerContainerFactory")
	public ResponseEntity<Response> consumeCustomerJson(@Payload String customer) {
		long startTime = System.currentTimeMillis();
		LOGGER.info("Consumer Request Time : " + startTime);
		Response response = new Response();
		ObjectMapper mapper = new ObjectMapper();
		Customer customerObj;
		Customer customerFromTopic;
		try {
			customerObj = mapper.readValue(customer, Customer.class);
			customerFromTopic = customerObj;
			AuditLog auditLog = new AuditLog();
			auditLog.setCustomerNumber(customerObj.getCustomerNumber());
			auditLog.setPayload(customer);
			auditLogRepository.save(auditLog);
			String customerNumber = customerObj.getCustomerNumber();
			customerNumber = customerNumber.replaceFirst(".{4}$", "####");
			customerObj.setCustomerNumber(customerNumber);
			String birthDate = customerObj.getBirthDate();
			birthDate = birthDate.replaceFirst("^.{4}", "####");
			customerObj.setBirthDate(birthDate);
			String email = customerObj.getEmail();
			email = email.replaceFirst("^.{4}", "####");
			customerObj.setEmail(email);
			customerRepository.save(customerObj);
			response.setStatus(CustomerConstants.SUCCESS_MSG);
			String customerString = mapper.writeValueAsString(customerFromTopic);
			response.setMessage(customerString);
			long elapsedTime = System.currentTimeMillis() - startTime;
			LOGGER.info("Consumer Response Time : " + elapsedTime);
			responseEntity = new ResponseEntity<Response>(response, HttpStatus.OK);
			return responseEntity;
		} catch (Exception e) {
			ErrorLog errorLog = new ErrorLog();
			errorLog.setError_type("exception");
			errorLog.setError_description(e.getMessage().toString());
			errorLog.setPayload(customer);
			errorLogRepository.save(errorLog);
			response.setStatus(CustomerConstants.ERROR_MSG);
			response.setMessage(CustomerConstants.CONSUMER_ERROR);
			response.setError_type(e.getMessage());
			responseEntity = new ResponseEntity<>(response, HttpStatus.UNPROCESSABLE_ENTITY);
			return responseEntity;
		}
	}
}
