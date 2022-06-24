package com.spring.batch_microservice.controller;

import org.springframework.batch.core.launch.JobOperator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import com.spring.batch_microservice.dao.BatchDatabaseDAO;

@RestController
public class JobController {
	
	@Autowired
	private JobOperator jobOperator;
	
	@Autowired
	private BatchDatabaseDAO batchDatabaseDAO;
	
	@GetMapping("/skipJob")
	public long skipJob() throws Exception {
		
		int lastJobExecutionId = batchDatabaseDAO.latestJobExecutionIdSkipJob();
		System.out.println(lastJobExecutionId);
		
		String lastJobStatus = batchDatabaseDAO.latestJobExecutionStatusSkipJob();
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
	
	@GetMapping("/stopJob/{id}")
	public void stopJob(@PathVariable("id") long id) throws Exception {
		
		/*
		 * 'id' here is the id returned from the 
		 * startJob() method
		 */
		this.jobOperator.stop(id);
	}
}
