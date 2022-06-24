package com.spring.batch_microservice.scheduler;

import org.springframework.batch.core.launch.JobOperator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.spring.batch_microservice.dao.BatchDatabaseDAO;

@Component
public class JobScheduler {

	@Autowired
	private JobOperator jobOperator;
	
	@Autowired
	private BatchDatabaseDAO batchDatabaseDAO;
	
	// start the job at 1 PM everyday
	@Scheduled(cron = "#{@getSkipJobCronValue}")
	public void runJob() throws Exception {
		
		int lastJobExecutionId = batchDatabaseDAO.latestJobExecutionIdSkipJob();
		System.out.println(lastJobExecutionId);
		
		String lastJobStatus = batchDatabaseDAO.latestJobExecutionStatusSkipJob();
		System.out.println(lastJobStatus);
		
		// job has not been run before or it was not completed last time
		if(lastJobExecutionId == -1 || lastJobStatus.equals("COMPLETED")) {
			
			long date = System.currentTimeMillis();
			// start a new instance of the job ... returns the new execution id
			this.jobOperator.start("skipJob", "date="+date);
			
		} else {
			
			// restart the previous job .. returns the new execution id
			this.jobOperator.restart(lastJobExecutionId);
		}
	}
}
