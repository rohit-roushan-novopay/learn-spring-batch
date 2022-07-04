package com.spring.batch.service;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;

import org.springframework.batch.core.launch.JobOperator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.spring.batch.dao.BatchDatabaseDAO;

@Service
public class JobLaunchingService {

	@Autowired
	private JobOperator jobOperator;
	
	@Autowired
	private BatchDatabaseDAO batchDatabaseDAO;
	
	public long launchJob(String jobName, String executionDate) throws Exception {
		
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MMMM d, yyyy", Locale.ENGLISH);
		LocalDate date = LocalDate.parse(executionDate, formatter);
		
		// start a new instance of the job with the passed executionDate
		return this.jobOperator.start(jobName, "date="+date);
	}

	public long launchJob(String jobName) throws Exception {
		
		int lastJobExecutionId = batchDatabaseDAO.latestJobExecutionId(jobName);
		System.out.println(lastJobExecutionId);
		
		String lastJobStatus = batchDatabaseDAO.latestJobExecutionStatus(jobName);
		System.out.println(lastJobStatus);
		
		// job has not been run before or it was not completed last time
		if(lastJobExecutionId == -1 || lastJobStatus.equals("COMPLETED")) {
			
			long date = System.currentTimeMillis();
			// start a new instance of the job ... returns the new execution id
			return this.jobOperator.start(jobName, "date="+date);
			
		} else {
			
			// restart the previous job .. returns the new execution id
			return this.jobOperator.restart(lastJobExecutionId);
		}
	}
	
	public void stopJob(String jobName) throws Exception {
		
		int lastJobExecutionId = batchDatabaseDAO.latestJobExecutionId(jobName);
		System.out.println(lastJobExecutionId);
		
		this.jobOperator.stop(lastJobExecutionId);
	}

	public HashMap<String, String> fetchJobScheduleDetails() {
		return batchDatabaseDAO.fetchJobScheduleDetails();
	}

	public void changeSchedule(String jobName, String cronExpression) {
		batchDatabaseDAO.changeSchedule(jobName, cronExpression);
	}

	public List<String> getOrderedStepsFromDB(String jobName) {
		return batchDatabaseDAO.getOrderedSteps(jobName);
	}
}
