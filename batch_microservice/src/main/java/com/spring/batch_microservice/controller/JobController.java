package com.spring.batch_microservice.controller;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ScheduledFuture;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.TriggerContext;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import com.spring.batch_microservice.service.JobLaunchingService;

@RestController
public class JobController {
	
	@Autowired
	private JobLaunchingService jobLaunchingService;
	
	@Autowired
    private ThreadPoolTaskScheduler threadPoolTaskScheduler;
	
	private List<ScheduledFuture> scheduledJobs = new ArrayList<>();
	
	@GetMapping("/skipJob")
	public void skipJob() {
		
		scheduledJobs.add(this.threadPoolTaskScheduler.schedule((new Runnable() {
			
			@Override
			public void run() {
				try {
					jobLaunchingService.launchSkipJob();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}), new Trigger() {
			
			@Override
			public Date nextExecutionTime(TriggerContext triggerContext) {
				
				// get the next execution time from db
				String cronExpression = jobLaunchingService.getCronExp("skipJob");
				
				CronExpression cronTrigger = CronExpression.parse(cronExpression);
				LocalDateTime next = cronTrigger.next(LocalDateTime.now());
				
				return Date.from(next.atZone(ZoneId.systemDefault()).toInstant());
			}
		}));
	}
	
	@GetMapping("/stopJob/{jobName}")
	public void stopJob(@PathVariable("jobName") String jobName) throws Exception {
		
		this.jobLaunchingService.stopJob(jobName);
	}
}
