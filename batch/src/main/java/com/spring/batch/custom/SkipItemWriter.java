package com.spring.batch.custom;

import java.util.List;

import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.spring.batch.pojo.Customer;
import com.spring.batch.exception.CustomRetryableException;

@Component
public class SkipItemWriter implements ItemWriter<Customer> {
	
	@Autowired
	private JdbcTemplate jdbcTemplate;
	
	@Override
	public void write(List<? extends Customer> items) throws Exception {
		
		for(Customer customer : items) {
			
			if(customer.getFirstName().startsWith("A")) {
				
				throw new CustomRetryableException("Skipping this item: " + customer);
			} else {
				
				// perform jdbc insert into new_customer table
				jdbcTemplate.update("INSERT INTO new_customer (firstName, lastName, birthdate) VALUES (?, ?, ?)",
					    customer.getFirstName(), customer.getLastName(), customer.getbirthdate());
			}
		}
	}

}
