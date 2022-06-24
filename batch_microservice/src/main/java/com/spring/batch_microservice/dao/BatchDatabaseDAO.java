package com.spring.batch_microservice.dao;

import java.util.List;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
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
	
	public String getSkipJobSchedule() {
		
		List<JobSchedule> scheduleList =  jdbcTemplate.query("SELECT * FROM schedules ORDER BY ID DESC", new JobScheduleRowMapper());
		List<JobSchedule> skipJobScheduleList = scheduleList.stream().filter(j -> j.getJobName().equals("skipJob")).collect(Collectors.toList());

		if(skipJobScheduleList.size() > 0) return skipJobScheduleList.get(0).getSchedule();
		else return null;
	}
	
	public int latestJobInstanceIdMTJob() {
		
		List<JobInstance> jobList =  jdbcTemplate.query("SELECT * FROM batch_job_instance ORDER BY JOB_INSTANCE_ID DESC", new JobInstanceRowMapper());
		
		List<JobInstance> retryJobList = jobList.stream().filter(j -> j.getJobName().equals("multithreadedJob")).collect(Collectors.toList());
			
		if(retryJobList.size() > 0) return retryJobList.get(0).getId();
		else return -1;
	}
	
	public String latestJobExecutionStatusMTJob() {
		
		List<JobExecution> jobExecutionList =  jdbcTemplate.query("SELECT * FROM batch_job_execution ORDER BY JOB_EXECUTION_ID DESC", new JobExecutionRowMapper());
		
		int lastJobInstanceId = latestJobInstanceIdMTJob();
		
		if(lastJobInstanceId == -1) return null;
		
		return jobExecutionList.stream().filter(j -> j.getJob_instance_id() == lastJobInstanceId)
				.collect(Collectors.toList()).get(0).getStatus();
	}
	
	public int latestJobExecutionIdMTJob() {
		
		List<JobExecution> jobExecutionList =  jdbcTemplate.query("SELECT * FROM batch_job_execution ORDER BY JOB_EXECUTION_ID DESC", new JobExecutionRowMapper());
		
		int lastJobInstanceId = latestJobInstanceIdMTJob();
		
		if(lastJobInstanceId == -1) return -1;
		
		return jobExecutionList.stream().filter(j -> j.getJob_instance_id() == lastJobInstanceId)
				.collect(Collectors.toList()).get(0).getId();
	}
	
	public int latestJobInstanceIdRetryJob() {
		
		List<JobInstance> jobList =  jdbcTemplate.query("SELECT * FROM batch_job_instance ORDER BY JOB_INSTANCE_ID DESC", new JobInstanceRowMapper());
		
		List<JobInstance> retryJobList = jobList.stream().filter(j -> j.getJobName().equals("retryJob")).collect(Collectors.toList());
			
		if(retryJobList.size() > 0) return retryJobList.get(0).getId();
		else return -1;
	}
	
	public String latestJobExecutionStatusRetryJob() {
		
		List<JobExecution> jobExecutionList =  jdbcTemplate.query("SELECT * FROM batch_job_execution ORDER BY JOB_EXECUTION_ID DESC", new JobExecutionRowMapper());
		
		int lastJobInstanceId = latestJobInstanceIdRetryJob();
		
		if(lastJobInstanceId == -1) return null;
		
		return jobExecutionList.stream().filter(j -> j.getJob_instance_id() == lastJobInstanceId)
				.collect(Collectors.toList()).get(0).getStatus();
	}
	
	public int latestJobExecutionIdRetryJob() {
		
		List<JobExecution> jobExecutionList =  jdbcTemplate.query("SELECT * FROM batch_job_execution ORDER BY JOB_EXECUTION_ID DESC", new JobExecutionRowMapper());
		
		int lastJobInstanceId = latestJobInstanceIdRetryJob();
		
		if(lastJobInstanceId == -1) return -1;
		
		return jobExecutionList.stream().filter(j -> j.getJob_instance_id() == lastJobInstanceId)
				.collect(Collectors.toList()).get(0).getId();
	}
	
	public int latestJobInstanceIdSkipJob() {
		
		List<JobInstance> jobList =  jdbcTemplate.query("SELECT * FROM batch_job_instance ORDER BY JOB_INSTANCE_ID DESC", new JobInstanceRowMapper());
		
		List<JobInstance> retryJobList = jobList.stream().filter(j -> j.getJobName().equals("skipJob")).collect(Collectors.toList());
			
		if(retryJobList.size() > 0) return retryJobList.get(0).getId();
		else return -1;
	}
	
	public String latestJobExecutionStatusSkipJob() {
		
		List<JobExecution> jobExecutionList =  jdbcTemplate.query("SELECT * FROM batch_job_execution ORDER BY JOB_EXECUTION_ID DESC", new JobExecutionRowMapper());
		
		int lastJobInstanceId = latestJobInstanceIdSkipJob();
		
		if(lastJobInstanceId == -1) return null;
		
		return jobExecutionList.stream().filter(j -> j.getJob_instance_id() == lastJobInstanceId)
				.collect(Collectors.toList()).get(0).getStatus();
	}
	
	public int latestJobExecutionIdSkipJob() {
		
		List<JobExecution> jobExecutionList =  jdbcTemplate.query("SELECT * FROM batch_job_execution ORDER BY JOB_EXECUTION_ID DESC", new JobExecutionRowMapper());
		
		int lastJobInstanceId = latestJobInstanceIdSkipJob();
		
		if(lastJobInstanceId == -1) return -1;
		
		return jobExecutionList.stream().filter(j -> j.getJob_instance_id() == lastJobInstanceId)
				.collect(Collectors.toList()).get(0).getId();
	}
	
	public int latestJobInstanceIdRestartJob() {
		
		List<JobInstance> jobList =  jdbcTemplate.query("SELECT * FROM batch_job_instance ORDER BY JOB_INSTANCE_ID DESC", new JobInstanceRowMapper());
		
		List<JobInstance> restartJobList = jobList.stream().filter(j -> j.getJobName().equals("restartJob")).collect(Collectors.toList());
			
		if(restartJobList.size() > 0) return restartJobList.get(0).getId();
		else return -1;
	}
	
	public String latestJobExecutionStatusRestartJob() {
		
		List<JobExecution> jobExecutionList =  jdbcTemplate.query("SELECT * FROM batch_job_execution ORDER BY JOB_EXECUTION_ID DESC", new JobExecutionRowMapper());
		
		int lastJobInstanceId = latestJobInstanceIdRestartJob();
		
		if(lastJobInstanceId == -1) return null;
		
		return jobExecutionList.stream().filter(j -> j.getJob_instance_id() == lastJobInstanceId)
				.collect(Collectors.toList()).get(0).getStatus();
	}
	
	public int latestJobExecutionIdRestartJob() {
		
		List<JobExecution> jobExecutionList =  jdbcTemplate.query("SELECT * FROM batch_job_execution ORDER BY JOB_EXECUTION_ID DESC", new JobExecutionRowMapper());
		
		int lastJobInstanceId = latestJobInstanceIdRestartJob();
		
		if(lastJobInstanceId == -1) return -1;
		
		return jobExecutionList.stream().filter(j -> j.getJob_instance_id() == lastJobInstanceId)
				.collect(Collectors.toList()).get(0).getId();
	}
}
