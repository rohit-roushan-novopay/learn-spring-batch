package com.spring_poc.spring_batch.conf;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {
	
	@Autowired
	private JobBuilderFactory jobBuilderFactory;
	
	@Autowired
	private StepBuilderFactory stepBuilderFactory;
	
	@Bean
	public Tasklet tasklet() {
		return new CountingTasklet();
	}
	
	@Bean
	public Step step1() {
		
		return stepBuilderFactory.get("step1").tasklet(new Tasklet() {
			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				
				System.out.println("This is step1");
				return RepeatStatus.FINISHED;
			}
		}).build();
	}
	
	@Bean
	public Step step2() {
		
		return stepBuilderFactory.get("step2").tasklet(new Tasklet() {
			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				
				System.out.println("This is step2");
				return RepeatStatus.FINISHED;
			}
		}).build();
	}
	
	@Bean
	public Step step3() {
		
		return stepBuilderFactory.get("step3").tasklet(new Tasklet() {
			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				
				System.out.println("This is step3");
				return RepeatStatus.FINISHED;
			}
		}).build();
	}
	
	@Bean
	public Step step4() {
		return stepBuilderFactory.get("step4").tasklet(tasklet()).build();
	}
	
	@Bean
	public Step step5() {
		return stepBuilderFactory.get("step5").tasklet(tasklet()).build();
	}
	
	@Bean
	public Step step6() {
		return stepBuilderFactory.get("step6").tasklet(tasklet()).build();
	}
	
	@Bean
	public Step step7() {
		return stepBuilderFactory.get("step7").tasklet(tasklet()).build();
	}
	
	/*
	 * A job represents a flow that processing will take place through 
	 * these states. Each step represents a state. A job may have many steps.
	 * There are two types of steps:
	 * 1. Tasklet -- Single interface with a single method. Spring batch runs 
	 *               this single step.
	 *               There are predefined tasklets also.
	 * 2. Chunk based step.
	 *    a. Item Reader -- input for the step
	 *    b. Item Processor(option)
	 *    c. Item Writer -- output of the step
	 *    
	 * Spring batch stores the state of the job in a job repository.
	 * Out of the box, spring batch provides two impls of this repository:
	 * 1. In memory using hashmaps -- default.
	 * 2. JDBC based.
	 */
	
	@Bean
	public Job transitionJob() {
		
		// split -- execute multiple flows in parallel
//		return jobBuilderFactory.get("transitionJob").start(flow1())
//								.split(new SimpleAsyncTaskExecutor()).add(flow2())
//								.end()
//								.build();
		
		return jobBuilderFactory.get("transitionJob").start(step4())
				.next(decider())
				.from(decider()).on("ODD").to(step1())
				.from(decider()).on("EVEN").to(step2())
				.from(step1()).on("*").to(decider())
				.end()
				.build();
	}
	
	
	/*
	 * eg. let's say we have to do this at the beginning of each job
	 * 1. send an email.
	 * 2. initialize a database
	 * 3. write something on a log file
	 * we can encapsulate all these in a flow and append it each job
	 */
	@Bean
	public Flow flow1() {
		
		FlowBuilder<Flow> flowBuilder = new FlowBuilder<>("flow1");
		
		flowBuilder.start(step4()).next(step5()).end();
		
		return flowBuilder.build();
	}
	
	@Bean
	public Flow flow2() {
		
		FlowBuilder<Flow> flowBuilder = new FlowBuilder<>("flow2");
		
		flowBuilder.start(step6()).next(step7()).end();
		
		return flowBuilder.build();
	}
	
	public static class CountingTasklet implements Tasklet {
		
		@Override
		public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
			
			System.out.println(chunkContext.getStepContext().getStepName() + " : " + Thread.currentThread().getName());
			return RepeatStatus.FINISHED;
		}
	}
	
	@Bean
	public JobExecutionDecider decider() {
		return new OddDecider();
	}
	
	/* Another option to control the order of execution of jobs
	 * Deciders do not get entry in the job repository
	 */
	public static class OddDecider implements JobExecutionDecider {
		
		private int count = 0;
		@Override
		public FlowExecutionStatus decide(JobExecution jobExecution, StepExecution stepExecution) {
			
			// step execution is the last step executed before the decider was called, it can be null
			count++;
			
			if(count % 2 == 0) return new FlowExecutionStatus("EVEN");
			else return new FlowExecutionStatus("ODD");
		}
	}
}


