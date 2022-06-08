package com.spring_poc.spring_batch.conf;

import java.util.Arrays;
import java.util.List;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.mail.javamail.JavaMailSender;

import com.spring_poc.spring_batch.listeners.ChunkListener;
import com.spring_poc.spring_batch.listeners.JobListener;

@Configuration
@EnableBatchProcessing
public class ListenerJobConfiguration {

	@Autowired
	private JobBuilderFactory jobBuilderFactory;
	
	@Autowired
	private StepBuilderFactory stepBuilderFactory;
	
	@Bean
	public ItemReader<String> reader() {
		
		// returns a single item/unit of processing
		return new StatelessItemReader(Arrays.asList("one", "two", "three"));
	}
	
	@Bean
	public ItemWriter<String> writer() {
		
		return new ItemWriter<String>() {
			@Override
			public void write(List<? extends String> items) throws Exception {
				
				for(String item : items) {
					System.out.println("Writing item : " + item);
				}
				
			}
		};
	}
	
	/*
	 * Chunk based step. Chunk size is 2
	 * There are 3 items to read so it will be divided into 2 chunks -- chunk listener will be activated twice
	 * 1. ItemReader reads the records till the chunk limit
	 * 2. ItemProcessor is called for all the records in the chunk
	 * 3. ItemWriter is called for all the records in the chunk
	 * <String, String> --> generics for the ItemReader and ItemWriter
	 */
	@Bean
	public Step step1A() {
		
		return stepBuilderFactory.get("step1A").<String, String> chunk(2)
						.faultTolerant()
						.listener(new ChunkListener())
						.reader(reader())
						.writer(writer())
						.build();
	}
	
	@Bean
	public Job listenerJob(JavaMailSender javaMailSender) {
		
		return jobBuilderFactory.get("listenerJob").start(step1A())
						.listener(new JobListener(javaMailSender))
						.build();
	}
}