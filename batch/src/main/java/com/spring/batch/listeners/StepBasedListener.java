package com.spring.batch.listeners;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;

public class StepBasedListener implements StepExecutionListener {

	private int read_count = 0;
	private int write_count = 0;
	private int job_completion_percentage = 0;
	
	private JavaMailSender javaMailSender;
	
    public StepBasedListener (JavaMailSender javaMailSender) {
        this.javaMailSender = javaMailSender;
    }
	
	@Override
	public void beforeStep(StepExecution stepExecution) {
		
		System.out.println("Step " + stepExecution.getStepName() + " has started execution.");
		SimpleMailMessage mail = getSimpleMailMessage("Step " + stepExecution.getStepName() + " has started execution.", "");
		this.javaMailSender.send(mail);
	}

	@Override
	public ExitStatus afterStep(StepExecution stepExecution) {
		
		String stepName = stepExecution.getStepName();
		String stepSummary = stepExecution.getSummary();
		
		read_count += stepExecution.getReadCount();
		write_count += stepExecution.getWriteCount();
		job_completion_percentage = (write_count * 100)/1003196;
		
		System.out.println("Step " + stepName + " executed successfully");
		System.out.println("Step Summary: " + stepSummary);
		System.out.println("No of items read till now: " + read_count);
		System.out.println("No of items written till now: " + write_count);
		System.out.println("Percentage Completion of Job: " + job_completion_percentage + "%");
		
		SimpleMailMessage mail = getSimpleMailMessage("Step " + stepName + " executed successfully",
				"Step Summary: " + stepSummary + "\n\n" + "No of items read till now: " + read_count + "\n\n"
				+ "No of items written till now: " + write_count + "\n\n" 
				+ "Percentage Completion of Job: " + job_completion_percentage + "%");

		this.javaMailSender.send(mail);

		return stepExecution.getExitStatus();
	}

	private SimpleMailMessage getSimpleMailMessage(String subject, String text) {
		SimpleMailMessage mail = new SimpleMailMessage();

		mail.setTo("rohit.roushan@novopay.in");
		mail.setSubject(subject);
		mail.setText(text);
		return mail;
	}
}
