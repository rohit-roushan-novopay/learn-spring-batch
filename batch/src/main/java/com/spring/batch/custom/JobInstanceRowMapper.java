package com.spring.batch.custom;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

import com.spring.batch.pojo.JobInstance;

public class JobInstanceRowMapper implements RowMapper<JobInstance>{

	@Override
	public JobInstance mapRow(ResultSet resultSet, int rowNum) throws SQLException {
	
		return new JobInstance(resultSet.getInt("job_instance_id"), resultSet.getInt("version"), 
				resultSet.getString("job_name"), resultSet.getString("job_key"));
	}

}
