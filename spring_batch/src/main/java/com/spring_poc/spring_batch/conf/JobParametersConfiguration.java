package com.spring_poc.spring_batch.conf;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableBatchProcessing
public class JobParametersConfiguration {

	@Autowired
	private JobBuilderFactory jobBuilderFactory;
	
	@Autowired
	private StepBuilderFactory stepBuilderFactory;
	
	/*
	 * @StepScope : tells spring to instantiates this object once the step that is using it, calls it
	 * unlike typical spring beans that are all created on startup.
	 * 
	 * This is done so that we can pass the job parameter --> currentTime in this case
	 */
	@Bean
	@StepScope
	public Tasklet parametersTasklet(@Value("#{jobParameters['executionTime']}") String executionTime) {
		
		return new Tasklet() {
			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				
				System.out.println(executionTime);
				return RepeatStatus.FINISHED;
			}
		};
	}
	
	@Bean
	public Step step1B() {
		
		return stepBuilderFactory.get("step1B").tasklet(parametersTasklet(null)).build();
		
		// return stepBuilderFactory.get("step1B").tasklet(parametersTasklet(executionTime)).build();

	}
	
	@Bean
	public Job parametersJob() {
		return jobBuilderFactory.get("parametersJob").start(step1B()).build();
	}
	
}
