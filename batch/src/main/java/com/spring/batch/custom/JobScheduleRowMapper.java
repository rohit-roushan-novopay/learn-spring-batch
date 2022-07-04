package com.spring.batch.custom;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

import com.spring.batch.pojo.JobSchedule;

public class JobScheduleRowMapper implements RowMapper<JobSchedule>{

	@Override
	public JobSchedule mapRow(ResultSet resultSet, int rowNum) throws SQLException {
	
		return new JobSchedule(resultSet.getInt("id"), resultSet.getString("jobGroup"), resultSet.getString("jobName"), 
				resultSet.getString("cronExpression"));
	}

}
