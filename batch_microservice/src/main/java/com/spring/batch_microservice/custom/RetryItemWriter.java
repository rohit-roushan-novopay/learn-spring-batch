package com.spring.batch_microservice.custom;

import java.util.List;

import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.spring.springbatch.exception.CustomRetryableException;
import com.spring.springbatch.pojo.Customer;

@Component
public class RetryItemWriter implements ItemWriter<Customer> {

	private int attemptCount = 0;
	
	@Autowired
	private JdbcTemplate jdbcTemplate;
	
	@Override
	public void write(List<? extends Customer> items) throws Exception {
		
		for(Customer customer : items) {
			
			if(customer.getFirstName().startsWith("A")) {
				attemptCount++;
				
				if(attemptCount >= 5) {
					
					System.out.println("Success!");
					attemptCount = 0;
					
					// perform jdbc insert into new_customer table
					jdbcTemplate.update("INSERT INTO new_customer (firstName, lastName, birthdate) VALUES (?, ?, ?)",
						    customer.getFirstName(), customer.getLastName(), customer.getBirthDate());
				}
				else {
					System.out.println("Writing of item " + customer.getId() + " failed");
					System.out.println("Attempt Count: " + attemptCount);
					throw new CustomRetryableException("Write failed.  Attempt:" + attemptCount);
				}
			} else {
				
				// perform jdbc insert into new_customer table
				jdbcTemplate.update("INSERT INTO new_customer (firstName, lastName, birthdate) VALUES (?, ?, ?)",
					    customer.getFirstName(), customer.getLastName(), customer.getBirthDate());
			}
		}
	}

}
