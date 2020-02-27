package com.kafkaconsumer.KafkaConsumer;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.junit4.SpringRunner;
import com.constants.CustomerConstants;
import com.response.Response;
import com.service.KafkaConsumerService;

@RunWith(SpringRunner.class)
@SpringBootTest
public class KafkaConsumerApplicationTests {
	@Test
	public void testConsumeCustomerFromTopic() {
		Response response = new Response();
		response.setStatus("Success");
		ResponseEntity<Response> responseEntity = new ResponseEntity<Response>(response, HttpStatus.OK);
		KafkaConsumerService kafkaConsumerService = mock(KafkaConsumerService.class);
		when(kafkaConsumerService.consumeCustomerFromTopic()).thenReturn(responseEntity);
		assertEquals(CustomerConstants.SUCCESS_MSG,
				kafkaConsumerService.consumeCustomerFromTopic().getBody().getStatus().toString());
	}
}
