package com.spring.batch.conf;

import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer;
import org.springframework.batch.core.configuration.support.JobRegistryBeanPostProcessor;
import org.springframework.batch.core.converter.DefaultJobParametersConverter;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

@Configuration
public class JobOperatorConfiguration extends DefaultBatchConfigurer implements ApplicationContextAware {

	@Autowired
	private JobExplorer jobExplorer;
	
	@Autowired
	private JobRepository jobRepository;
	
	@Autowired
	private JobRegistry jobRegistry;
	
	@Autowired
	private JobLauncher jobLauncher;
	
	private ApplicationContext applicationContext;

	/*
	 * Default implementation is provided by the DefaultBatchConfigurer class
	 * Here we are overriding the default implementation as we want the job to run
	 * asynchronously. 
	 */
	@Override
	public JobLauncher getJobLauncher() {
		
		SimpleJobLauncher jobLauncher = null;
		try {
			
			jobLauncher = new SimpleJobLauncher();
			jobLauncher.setJobRepository(jobRepository);
			jobLauncher.setTaskExecutor(new SimpleAsyncTaskExecutor());
			jobLauncher.afterPropertiesSet();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return jobLauncher;
	}
	
	@Bean
	public JobRegistryBeanPostProcessor jobRegistrar() throws Exception {
		
		JobRegistryBeanPostProcessor registrar = new JobRegistryBeanPostProcessor();
		
		// JobRegistry is simple key value pairs, key = job names and values = references of the job
		// spring batch gives a job registry out of the box, however no jobs are registered in the job registry by default
		registrar.setJobRegistry(this.jobRegistry);
		
		// This will register all the jobs within the application context so that they are available for the
		// jobOperator when it is the time to launch the job
		registrar.setBeanFactory(this.applicationContext.getAutowireCapableBeanFactory());
		
		registrar.afterPropertiesSet();
		
		return registrar;
	}
	
	@Bean
	public JobOperator jobOperator() throws Exception {
		
		SimpleJobOperator simpleJobOperator = new SimpleJobOperator();
		
		simpleJobOperator.setJobLauncher(this.jobLauncher);
		simpleJobOperator.setJobParametersConverter(new DefaultJobParametersConverter());
		simpleJobOperator.setJobRepository(this.jobRepository);
		simpleJobOperator.setJobExplorer(this.jobExplorer);
		simpleJobOperator.setJobRegistry(this.jobRegistry);
		
		simpleJobOperator.afterPropertiesSet();
		
		return simpleJobOperator;
	}
	
	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}
}
