package com.spring.batch.dao;

import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.spring.batch.custom.JobExecutionRowMapper;
import com.spring.batch.custom.JobInstanceRowMapper;
import com.spring.batch.custom.JobScheduleRowMapper;
import com.spring.batch.pojo.JobExecution;
import com.spring.batch.pojo.JobInstance;
import com.spring.batch.pojo.JobSchedule;

@Repository
public class BatchDatabaseDAO {

	@Autowired
	private JdbcTemplate jdbcTemplate;
	
	public int latestJobInstanceId(String jobName) {
		
		List<JobInstance> jobList =  jdbcTemplate.query("SELECT * FROM batch_job_instance ORDER BY JOB_INSTANCE_ID DESC", new JobInstanceRowMapper());
		
		List<JobInstance> retryJobList = jobList.stream().filter(j -> j.getJobName().equals(jobName)).collect(Collectors.toList());
			
		if(retryJobList.size() > 0) return retryJobList.get(0).getId();
		else return -1;
	}
	
	public String latestJobExecutionStatus(String jobName) {
		
		List<JobExecution> jobExecutionList =  jdbcTemplate.query("SELECT * FROM batch_job_execution ORDER BY JOB_EXECUTION_ID DESC", new JobExecutionRowMapper());
		
		int lastJobInstanceId = latestJobInstanceId(jobName);
		
		if(lastJobInstanceId == -1) return null;
		
		return jobExecutionList.stream().filter(j -> j.getJob_instance_id() == lastJobInstanceId)
				.collect(Collectors.toList()).get(0).getStatus();
	}
	
	public int latestJobExecutionId(String jobName) {
		
		List<JobExecution> jobExecutionList =  jdbcTemplate.query("SELECT * FROM batch_job_execution ORDER BY JOB_EXECUTION_ID DESC", new JobExecutionRowMapper());
		
		int lastJobInstanceId = latestJobInstanceId(jobName);
		
		if(lastJobInstanceId == -1) return -1;
		
		return jobExecutionList.stream().filter(j -> j.getJob_instance_id() == lastJobInstanceId)
				.collect(Collectors.toList()).get(0).getId();
	}

	public HashMap<String, String> fetchJobScheduleDetails() {
		
		HashMap<String, String> jobScheduleHMap = new HashMap<>();
		List<JobSchedule> jobSchedules =  jdbcTemplate.query("SELECT * FROM batch_job_schedule", new JobScheduleRowMapper());
		
		for(JobSchedule jobSchedule : jobSchedules) {
			jobScheduleHMap.put(jobSchedule.getJobName(), jobSchedule.getCronExpression());
		}
		return jobScheduleHMap;
	}

	public void changeSchedule(String jobName, String cronExpression) {
		jdbcTemplate.update("update batch_jobgroup_details set cronExpression = ? where jobName = ?", cronExpression, jobName);
	}

	public List<String> getOrderedSteps(String jobName) {
		
		List<String> stepsOrder = jdbcTemplate.queryForList("SELECT stepName FROM batch_job_stepsorder WHERE jobName='" + jobName +"' ORDER BY stepSequence", String.class);

		return stepsOrder;
	}
}