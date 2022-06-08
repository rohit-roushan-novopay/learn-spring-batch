package com.spring_poc.spring_batch.conf;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.PassThroughLineAggregator;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.batch.item.xml.StaxEventItemReader;
import org.springframework.batch.item.xml.StaxEventItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.oxm.xstream.XStreamMarshaller;

import com.spring_poc.spring_batch.listeners.ChunkListener;
import com.spring_poc.spring_batch.listeners.JobListener;
import com.spring_poc.spring_batch.pojo.Customer;
import com.spring_poc.spring_batch.pojo.CustomerLineAggregator;
import com.spring_poc.spring_batch.pojo.CustomerRowMapper;

@Configuration
@EnableBatchProcessing
public class JobJdbcConfiguration {
	
	@Autowired
	private JobBuilderFactory jobBuilderFactory;
	
	@Autowired
	private StepBuilderFactory stepBuilderFactory;
	
	@Autowired
	private DataSource dataSource;
	
	// Reader
//	@Bean
//	public JdbcCursorItemReader<Customer> cursorItemReader() {
//		
//		JdbcCursorItemReader<Customer> reader = new JdbcCursorItemReader<>();
//		
//		reader.setSql("select id, firstName, lastName, birthDate from customer order by lastName, firstName");
//		reader.setDataSource(this.dataSource);
//		reader.setRowMapper(new CustomerRowMapper());
//		
//		return reader;
//	}
	
	@Bean
	public JdbcPagingItemReader<Customer> pagingItemReader() {
		
		JdbcPagingItemReader<Customer> reader = new JdbcPagingItemReader<>();
		
		reader.setDataSource(dataSource);
		
		// set the fetch size same as chunk size
		reader.setFetchSize(10);
		reader.setRowMapper(new CustomerRowMapper());
		
		// spring batch generates a new sql statement for each page
		MySqlPagingQueryProvider queryProvider = new MySqlPagingQueryProvider();
		queryProvider.setSelectClause("id, firstName, lastName, birthDate");
		queryProvider.setFromClause("from customer");
		
		// used to order the data .. also keeps track of last value read
		Map<String, Order> sortKeys = new HashMap<>(1);
		sortKeys.put("id", Order.ASCENDING);
		queryProvider.setSortKeys(sortKeys);
		
		reader.setQueryProvider(queryProvider);
		
		return reader;
	}
	
	/*
	 * writes all the items read to a csv file
	 * does a single write for all of the items in one chunk
	 */
	@Bean
	public FlatFileItemWriter<Customer> flatFileItemWriter() throws Exception{
		
		FlatFileItemWriter<Customer> flatFileItemWriter = new FlatFileItemWriter<>();
		
		/* line aggregator determines how an object is mapped to a string that'll be written as a line 
		 * in your output file.
		 * PassThroughLineAggregator calls toString() on each object that is read.
		 * 
		 * We can also define our own line aggregators -- CustomerLineAggregator
		 */
		//flatFileItemWriter.setLineAggregator(new PassThroughLineAggregator<>());
		flatFileItemWriter.setLineAggregator(new CustomerLineAggregator());
		
		String customerOutputPath = File.createTempFile("customerOutput", ".out").getAbsolutePath();
		System.out.println(">> Output Path: " + customerOutputPath);
		
		flatFileItemWriter.setResource(new FileSystemResource(customerOutputPath));
		flatFileItemWriter.afterPropertiesSet();
		
		return flatFileItemWriter;
	}
	
	/*
	 * writes all the items read to a xml file
	 * does a single write for all of the items in one chunk
	 */
	@Bean
	public StaxEventItemWriter<Customer> xmlItemWriter() throws Exception {
		
		XStreamMarshaller marshaller = new XStreamMarshaller();
		
		Map<String, Class<?>> aliases = new HashMap<>();
		aliases.put("customer", Customer.class);
		
		marshaller.setAliases(aliases);
		
		StaxEventItemWriter<Customer> xmlItemWriter = new StaxEventItemWriter<>();
		
		xmlItemWriter.setRootTagName("customers");
		xmlItemWriter.setMarshaller(marshaller);
		
		String customerOutputPath = File.createTempFile("customerOutput", ".xml").getAbsolutePath();
	    System.out.println(">> Output Path: " + customerOutputPath);
	    
	    xmlItemWriter.setResource(new FileSystemResource(customerOutputPath));
	    
	    xmlItemWriter.afterPropertiesSet();
	    
	    return xmlItemWriter;
	}
	
	@Bean
	public CompositeItemWriter<Customer> compositeItemWriter() throws Exception {
		
		List<ItemWriter<? super Customer>> writers = new ArrayList<>();
		
		writers.add(flatFileItemWriter());
		writers.add(xmlItemWriter());
		
		CompositeItemWriter<Customer> compositeItemWriter = new CompositeItemWriter<>();
		
		compositeItemWriter.setDelegates(writers);
		compositeItemWriter.afterPropertiesSet();
		
		return compositeItemWriter;
	}
	
	@Bean
	public Step step1C() throws Exception{
		
		return stepBuilderFactory.get("step1C").<Customer, Customer> chunk(10)
						.faultTolerant()
						.listener(new ChunkListener())
						.reader(pagingItemReader())
						.writer(compositeItemWriter())
						.build();
	}
	
	@Bean
	public Job jdbcJob(JavaMailSender javaMailSender) throws Exception {
		
		return jobBuilderFactory.get("jdbcJob").start(step1C())
						.listener(new JobListener(javaMailSender))
						.build();
	}
}
