package com.spring.batch_microservice.custom;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

import com.spring.batch_microservice.pojo.JobSchedule;

public class JobScheduleRowMapper implements RowMapper<JobSchedule>{

	@Override
	public JobSchedule mapRow(ResultSet resultSet, int rowNum) throws SQLException {
	
		return new JobSchedule(resultSet.getInt("id"), resultSet.getString("jobName"), resultSet.getString("schedule"));
	}

}
