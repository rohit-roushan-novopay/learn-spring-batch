package com.spring.batch_microservice.dao;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Repository;

import com.spring.batch_microservice.custom.JobExecutionRowMapper;
import com.spring.batch_microservice.custom.JobInstanceRowMapper;
import com.spring.batch_microservice.custom.JobScheduleRowMapper;
import com.spring.batch_microservice.pojo.JobExecution;
import com.spring.batch_microservice.pojo.JobInstance;
import com.spring.batch_microservice.pojo.JobSchedule;

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

	public String getCronExp(String jobName) {
		
		List<JobSchedule> scheduleList =  jdbcTemplate.query("SELECT * FROM schedules ORDER BY ID DESC", new JobScheduleRowMapper());
		List<JobSchedule> skipJobScheduleList = scheduleList.stream().filter(j -> j.getJobName().equals(jobName)).collect(Collectors.toList());

		if(skipJobScheduleList.size() > 0) return skipJobScheduleList.get(0).getSchedule();
		else return null;
	}
}
