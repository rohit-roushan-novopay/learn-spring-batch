package com.spring.batch.listeners;

import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;

public class ChunkBasedListener implements ChunkListener{

	private int chunkSize = 1000;
	private JavaMailSender javaMailSender;
	
	public ChunkBasedListener (JavaMailSender javaMailSender) {
        this.javaMailSender = javaMailSender;
    }
	
	@Override
	public void beforeChunk(ChunkContext context) {}

	@Override
	public void afterChunk(ChunkContext context) {}

	@Override
	public void afterChunkError(ChunkContext context) {
		
		int no_of_items_processed_successfully = context.getStepContext().getStepExecution().getWriteCount();
		int no_of_chunks_processed_successfully = no_of_items_processed_successfully/chunkSize;
		int error_occurred_in_chunk = no_of_chunks_processed_successfully + 1;
		
		System.out.println("No of items successfully processed in this step: " + no_of_items_processed_successfully);
		System.out.println("No of chunks executed successfully in this step: " + no_of_chunks_processed_successfully);
		System.out.println("Error occurred while processing chunk no: " + error_occurred_in_chunk);
		
		System.out.println("Error occurred while executing step: " + context.getStepContext().getStepName());
		
		SimpleMailMessage mail = getSimpleMailMessage("Error occurred while executing step: " + context.getStepContext().getStepName(),
				"No of items successfully processed in this step: " + no_of_items_processed_successfully + "\n\n" 
				+ "No of chunks executed successfully in this step: " + no_of_chunks_processed_successfully + "\n\n"
				+ "Error occurred while processing chunk no: " + error_occurred_in_chunk);
		
		this.javaMailSender.send(mail);
	}
	
	private SimpleMailMessage getSimpleMailMessage(String subject, String text) {
		SimpleMailMessage mail = new SimpleMailMessage();

		mail.setTo("rohit.roushan@novopay.in");
		mail.setSubject(subject);
		mail.setText(text);
		return mail;
	}
}
