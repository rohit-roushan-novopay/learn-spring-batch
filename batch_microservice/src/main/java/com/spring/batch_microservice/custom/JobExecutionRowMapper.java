package com.spring.batch_microservice.custom;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

import com.spring.batch_microservice.pojo.JobExecution;

public class JobExecutionRowMapper implements RowMapper<JobExecution>{

	@Override
	public JobExecution mapRow(ResultSet resultSet, int rowNum) throws SQLException {
	
		return new JobExecution(resultSet.getInt("job_execution_id"), resultSet.getInt("version"), resultSet.getInt("job_instance_id"),
				resultSet.getDate("create_time"), resultSet.getDate("start_time"), resultSet.getDate("end_time"), 
				resultSet.getString("status"), resultSet.getString("exit_code"), resultSet.getString("exit_message"),
				resultSet.getDate("last_updated"), resultSet.getString("job_configuration_location"));
	}

}
