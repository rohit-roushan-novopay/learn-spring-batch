package com.spring_poc.spring_batch.listeners;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;

public class JobListener implements JobExecutionListener{

	private JavaMailSender mailSender;
	
	public JobListener(JavaMailSender mailSender) {
		this.mailSender = mailSender;
	}
	
	@Override
	public void beforeJob(JobExecution jobExecution) {
		
		String jobName = jobExecution.getJobInstance().getJobName();
		
		SimpleMailMessage mail = getSimpleMailMessage(jobName + "is starting", "Please be informed that " + jobName + " is starting");
		mailSender.send(mail);
	}

	@Override
	public void afterJob(JobExecution jobExecution) {
		
		String jobName = jobExecution.getJobInstance().getJobName();
		
		SimpleMailMessage mail = getSimpleMailMessage(jobName + "has ended", "Please be informed that " + jobName + " has ended");
		mailSender.send(mail);
	}
	
	private SimpleMailMessage getSimpleMailMessage(String subject, String text) {
		
		SimpleMailMessage mail = new SimpleMailMessage();
		
		mail.setTo("rohit.roushan@novopay.in");
		mail.setSubject(subject);
		mail.setText(text);
		
		return mail;
	}
}








