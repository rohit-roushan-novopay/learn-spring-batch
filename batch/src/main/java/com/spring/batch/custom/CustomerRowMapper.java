package com.spring.batch.custom;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

import com.spring.batch.pojo.Customer;

public class CustomerRowMapper implements RowMapper<Customer>{

	@Override
	public Customer mapRow(ResultSet resultSet, int rowNum) throws SQLException {
	
		return new Customer(resultSet.getLong("id"), resultSet.getString("firstName"), 
				resultSet.getString("lastName"), resultSet.getDate("birthDate"));
	}

}
