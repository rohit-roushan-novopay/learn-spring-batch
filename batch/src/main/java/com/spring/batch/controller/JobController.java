package com.spring.batch.controller;

import org.springframework.batch.core.launch.JobOperator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.spring.batch.dao.BatchDatabaseDAO;

@RestController
public class JobController {
	
	@Autowired
	private JobOperator jobOperator;
	
	@Autowired
	private BatchDatabaseDAO batchDatabaseDAO;
	
	@GetMapping("/batchJob")
	public long batchJob() throws Exception {
		
//		int lastJobExecutionId = batchDatabaseDAO.latestJobExecutionId("batchJob");
//		System.out.println(lastJobExecutionId);
//		
//		String lastJobStatus = batchDatabaseDAO.latestJobExecutionStatus("batchJob");
//		System.out.println(lastJobStatus);
//		
//		// job has not been run before or it was not completed last time
//		if(lastJobExecutionId == -1 || lastJobStatus.equals("COMPLETED")) {
//			
//			long date = System.currentTimeMillis();
//			// start a new instance of the job ... returns the new execution id
//			return this.jobOperator.start("batchJob", "date="+date);
//			
//		} else {
//			
//			// restart the previous job .. returns the new execution id
//			return this.jobOperator.restart(lastJobExecutionId);
//		}
		
		long date = System.currentTimeMillis();
		// start a new instance of the job ... returns the new execution id
		return this.jobOperator.start("batchJob", "date="+date);
	}

}
