package com.spring.batch_microservice;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableBatchProcessing
@EnableScheduling
public class BatchMicroserviceApplication {

	public static void main(String[] args) {
		SpringApplication.run(BatchMicroserviceApplication.class, args);
	}

}
