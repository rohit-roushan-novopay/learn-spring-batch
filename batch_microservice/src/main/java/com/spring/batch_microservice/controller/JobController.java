package com.spring.batch_microservice.controller;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ScheduledFuture;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.TriggerContext;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.spring.batch_microservice.service.JobLaunchingService;

@RestController
public class JobController {
	
	@Autowired
	private JobLaunchingService jobLaunchingService;
	
	@Autowired
    private ThreadPoolTaskScheduler threadPoolTaskScheduler;
	
	private List<ScheduledFuture> scheduledJobs = new ArrayList<>();
	
	/*
	 * Schedule Jobs on start
	 */
	@GetMapping("/")
	public void scheduleJobs() throws Exception {
		
		rescheduleJobs();
	}
	
	/*
	 * Passing execution date as a job parameter, when starting a new job
	 */
	@GetMapping("/startJob")
	public long startJob(@RequestParam("jobName") String jobName,
			@RequestParam("executionDate") String executionDate) throws Exception {
		
		return this.jobLaunchingService.launchJob(jobName, executionDate);
	}
	
	/*
	 * restarts a job
	 */
	@GetMapping("/restartJob")
	public long startJob(@RequestParam("jobName") String jobName) throws Exception {
		
		return this.jobLaunchingService.launchJob(jobName);
	}
	
	/*
	 * parameters: 
	 * 1. jobName : name of the job to be rescheduled
	 * 2. cronExpression: new schedule of the job
	 */
	@GetMapping("/changeSchedule")
	public void changeSchedule(@RequestParam("jobName") String jobName, 
			@RequestParam("cronExpression") String cronExpression) throws Exception {
		
		this.jobLaunchingService.changeSchedule(jobName, cronExpression);
		
		// rescheduling the jobs
		rescheduleJobs();
	}
	
	@GetMapping("/rescheduleJobs")
	public void rescheduleJobs() {
		
		// clear the existing schedule
		scheduledJobs.clear();
		
		HashMap<String, String> jobScheduleDetails = fetchJobScheduleDetails();
		
		for(String jobName : jobScheduleDetails.keySet()) {
			scheduledJobs.add(this.threadPoolTaskScheduler.schedule((new Runnable() {
				
				@Override
				public void run() {
					try {
						jobLaunchingService.launchJob(jobName);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}), new Trigger() {
				@Override
				public Date nextExecutionTime(TriggerContext triggerContext) {
					
					String cronExpression = jobScheduleDetails.get(jobName);
					
					CronExpression cronTrigger = CronExpression.parse(cronExpression);
					LocalDateTime next = cronTrigger.next(LocalDateTime.now());
					
					return Date.from(next.atZone(ZoneId.systemDefault()).toInstant());
				}
			}));
		}
	}

	/*
	 * Parameters:
	 * 1. jobName : Name of the job to be stopped
	 */
	@GetMapping("/stopJob/{jobName}")
	public void stopJob(@PathVariable("jobName") String jobName) throws Exception {
		
		this.jobLaunchingService.stopJob(jobName);
	}
	
	// helper method
	private HashMap<String, String> fetchJobScheduleDetails() {
		return this.jobLaunchingService.fetchJobScheduleDetails();
	}
}
