package com.dishant.java.kinesis.abcd;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import software.amazon.awssdk.regions.Region;

@RestController
@RequestMapping
public class Controller {

	@Autowired
	private Publisher publisher;

	@GetMapping
	public void abc(){
		publisher.publish("dev_notification_email", Region.of("ap-south-1"));
	}
}
