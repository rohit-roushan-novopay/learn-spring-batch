package com.spring.batch_microservice.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import com.spring.batch_microservice.service.JobSchedulingService;

@RestController
public class JobController {
	
	@Autowired
	private JobSchedulingService jobSchedulingService;
	
    private ThreadPoolTaskScheduler  threadPoolTaskScheduler;
	private List<ScheduledFuture> scheduledJobs = new ArrayList<>();
	
	@GetMapping("/skipJob/{cronExp}")
	public void skipJob(@PathVariable("cronExp") String cronExpression) {
	
		Trigger trigger = new CronTrigger(cronExpression);
		
		scheduledJobs.add(this.threadPoolTaskScheduler.schedule((new Runnable() {
			@Override
			public void run() {
				try {
					jobSchedulingService.launchSkipJob();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}), trigger));
	}
	
	@GetMapping("/stopJob/{jobName}")
	public void stopJob(@PathVariable("jobName") String jobName) throws Exception {
		
		this.jobSchedulingService.stopJob(jobName);
	}
}
