package com.spring_poc.spring_batch.pojo;

import org.springframework.batch.item.file.transform.LineAggregator;

import com.fasterxml.jackson.databind.ObjectMapper;

public class CustomerLineAggregator implements LineAggregator<Customer>{

	private ObjectMapper objectMapper = new ObjectMapper();
	
	@Override
	public String aggregate(Customer item) {
		
		try {
			
			// each line will be an independent json object
			return objectMapper.writeValueAsString(item);
		} catch (Exception e) {
			throw new RuntimeException("Unable to serialize Customer", e);
		}
	}

}
