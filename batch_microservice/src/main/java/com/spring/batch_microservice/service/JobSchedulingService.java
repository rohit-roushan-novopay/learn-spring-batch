package com.spring.batch_microservice.service;

import org.springframework.batch.core.launch.JobOperator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.spring.batch_microservice.dao.BatchDatabaseDAO;

@Service
public class JobSchedulingService {

	@Autowired
	private JobOperator jobOperator;
	
	@Autowired
	private BatchDatabaseDAO batchDatabaseDAO;
	
	public long launchSkipJob() throws Exception {
		
		int lastJobExecutionId = batchDatabaseDAO.latestJobExecutionId("skipJob");
		System.out.println(lastJobExecutionId);
		
		String lastJobStatus = batchDatabaseDAO.latestJobExecutionStatus("skipJob");
		System.out.println(lastJobStatus);
		
		// job has not been run before or it was not completed last time
		if(lastJobExecutionId == -1 || lastJobStatus.equals("COMPLETED")) {
			
			long date = System.currentTimeMillis();
			// start a new instance of the job ... returns the new execution id
			return this.jobOperator.start("skipJob", "date="+date);
			
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
}
