package com.spring.batch.conf;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.mail.javamail.JavaMailSender;

import com.spring.batch.custom.ColumnRangePartitioner;
import com.spring.batch.custom.CustomSkipListener;
import com.spring.batch.custom.CustomerRowMapper;
import com.spring.batch.custom.SkipItemWriter;
import com.spring.batch.exception.CustomRetryableException;
import com.spring.batch.listeners.ChunkBasedListener;
import com.spring.batch.listeners.StepBasedListener;
import com.spring.batch.pojo.Customer;
import com.spring.batch.service.JobLaunchingService;
import com.spring.batch.custom.RetryItemWriter;

@Configuration
public class JobConfiguration implements ApplicationContextAware {
	
	@Autowired
	private JobBuilderFactory jobBuilderFactory;
	
	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private DataSource dataSource;

	@Autowired
	private ApplicationContext applicationContext;
	
	@Bean
	public ColumnRangePartitioner partitioner() {
		ColumnRangePartitioner columnRangePartitioner = new ColumnRangePartitioner();

		columnRangePartitioner.setColumn("id");
		columnRangePartitioner.setDataSource(this.dataSource);
		columnRangePartitioner.setTable("customer");

		return columnRangePartitioner;
	}
	
	/*
	 *  writes all the items read into the database
	 *  does a single jdbc batch update for all of the items in the chunk
	 */
	@Bean
	public JdbcBatchItemWriter<Customer> jdbcItemWriter() {
		
		JdbcBatchItemWriter<Customer> itemWriter = new JdbcBatchItemWriter<>();
		itemWriter.setDataSource(this.dataSource);
		itemWriter.setSql("INSERT INTO NEW_CUSTOMER (firstName, lastName, birthdate) VALUES (:firstName, :lastName, :birthdate)");
		itemWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
		itemWriter.afterPropertiesSet();
		
		return itemWriter;
	}
	
	@Bean
	@StepScope
	public JdbcPagingItemReader<Customer> pagingItemReader(@Value("#{stepExecutionContext['minValue']}")Long minValue,
			@Value("#{stepExecutionContext['maxValue']}")Long maxValue) {
		
		System.out.println("reading " + minValue + " to " + maxValue);
		
		JdbcPagingItemReader<Customer> reader = new JdbcPagingItemReader<>();
		
		reader.setDataSource(dataSource);
		
		// set the fetch size same as chunk size
		reader.setFetchSize(1000);
		reader.setRowMapper(new CustomerRowMapper());
		
		// spring batch generates a new sql statement for each page
		MySqlPagingQueryProvider queryProvider = new MySqlPagingQueryProvider();
		queryProvider.setSelectClause("id, firstName, lastName, birthDate");
		queryProvider.setFromClause("from customer");
		queryProvider.setWhereClause("where id >= " + minValue + " and id < " + maxValue);
		
		// used to order the data .. also keeps track of last value read
		Map<String, Order> sortKeys = new HashMap<>(1);
		sortKeys.put("id", Order.ASCENDING);
		queryProvider.setSortKeys(sortKeys);
		
		reader.setQueryProvider(queryProvider);
		
		return reader;
	}
	
	// Partitioning : Master step for step1S
	@Bean
	public Step step1M(JavaMailSender javaMailSender) throws Exception {
		return stepBuilderFactory.get("step1M")
				.partitioner(step1S(javaMailSender).getName(), partitioner())
				.step(step1S(javaMailSender))
				.gridSize(10)
				.taskExecutor(new SimpleAsyncTaskExecutor())
				.build();
	}
	
	// Partitioning : Slave step for step1M
	@Bean
	public Step step1S(JavaMailSender javaMailSender) throws Exception{
		
		return stepBuilderFactory.get("step1S").<Customer, Customer> chunk(1000)
						.reader(pagingItemReader(null, null))
						.writer(jdbcItemWriter())
						.faultTolerant()
						.listener(new StepBasedListener(javaMailSender))
						.listener(new ChunkBasedListener(javaMailSender))
						.build();
	}
	
	@Bean
	public Job batchJob(JavaMailSender javaMailSender) throws Exception {	
			
		return jobBuilderFactory.get("batchJob").start(step1M(javaMailSender)).build();
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}
}
