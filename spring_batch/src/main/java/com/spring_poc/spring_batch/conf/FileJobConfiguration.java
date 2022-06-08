package com.spring_poc.spring_batch.conf;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.mail.javamail.JavaMailSender;

import com.spring_poc.spring_batch.listeners.ChunkListener;
import com.spring_poc.spring_batch.listeners.JobListener;
import com.spring_poc.spring_batch.pojo.Customer;
import com.spring_poc.spring_batch.pojo.CustomerFieldSetMapper;

@Configuration
@EnableBatchProcessing
public class FileJobConfiguration {
	
	@Autowired
	private JobBuilderFactory jobBuilderFactory;
	
	@Autowired
	private StepBuilderFactory stepBuilderFactory;
	
	@Value("classpath*:/data/customer*.csv")
	private Resource[] inputFiles;
	
	@Autowired
	private DataSource dataSource;
	
	/* 
	 * For each file, it will set the resource on the flatFileItemReader
	 * and keep calling read() until it returns null.
	 * When the reader returns null, it will set the next file and continue until all of the files are read
	 */
	@Bean
	public MultiResourceItemReader<Customer> multiResourceItemReader(){
		
		MultiResourceItemReader<Customer> multiResourceItemReader = new MultiResourceItemReader<>();
		
		multiResourceItemReader.setDelegate(flatFileItemReader());
		multiResourceItemReader.setResources(inputFiles);
		
		return multiResourceItemReader;
	}
	
	@Bean
	public FlatFileItemReader<Customer> flatFileItemReader() {
		
		FlatFileItemReader<Customer> reader = new FlatFileItemReader<>();
		
		reader.setLinesToSkip(1);
		
		DefaultLineMapper<Customer> customerLineMapper = new DefaultLineMapper<>();
		
		DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
		tokenizer.setNames(new String[] {"id", "firstName", "lastName", "birthDate"});
		
		customerLineMapper.setLineTokenizer(tokenizer);
		customerLineMapper.setFieldSetMapper(new CustomerFieldSetMapper());
		customerLineMapper.afterPropertiesSet();
		
		reader.setLineMapper(customerLineMapper);
		
		return reader;
	}
	
	/*
	 *  writes all the items read into the db
	 *  does a single jdbc batch update for all of the items in the chunk
	 */
	@Bean
	public JdbcBatchItemWriter<Customer> jdbcItemWriter() {
		
		JdbcBatchItemWriter<Customer> itemWriter = new JdbcBatchItemWriter<>();
		itemWriter.setDataSource(this.dataSource);
		itemWriter.setSql("INSERT INTO CUSTOMER VALUES (:id, :firstName, :lastName, :birthDate)");
		itemWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
		itemWriter.afterPropertiesSet();
		
		return itemWriter;
	}
	
	@Bean
	public Step step1D() {
		
		return stepBuilderFactory.get("step1D").<Customer, Customer> chunk(10)
						.faultTolerant()
						.listener(new ChunkListener())
						.reader(multiResourceItemReader())
						.writer(jdbcItemWriter())
						.build();
	}
	
	@Bean
	public Job fileJob(JavaMailSender javaMailSender) {
		
		return jobBuilderFactory.get("fileJob").start(step1D())
						.listener(new JobListener(javaMailSender))
						.build();
	}
}
