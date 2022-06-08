package com.spring_poc.spring_batch.conf;

import java.util.HashMap;
import java.util.Map;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.xml.StaxEventItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.oxm.xstream.XStreamMarshaller;

import com.spring_poc.spring_batch.listeners.ChunkListener;
import com.spring_poc.spring_batch.listeners.JobListener;
import com.spring_poc.spring_batch.pojo.Customer;

@Configuration
@EnableBatchProcessing
public class ReadXMLJobConfiguration {

	@Autowired
	private JobBuilderFactory jobBuilderFactory;
	
	@Autowired
	private StepBuilderFactory stepBuilderFactory;
	
	// Reader
	@Bean
	@StepScope
	public StaxEventItemReader<Customer> xmlItemReader(@Value("#{jobParameters['executionTime']}") String executionTime) {
				
		XStreamMarshaller unmarshaller = new XStreamMarshaller();
		
		Map<String, Class<?>> aliases = new HashMap<>();
		aliases.put("customer", Customer.class);
		
		unmarshaller.setAliases(aliases);

		StaxEventItemReader<Customer> reader = new StaxEventItemReader<>();

		reader.setResource(new ClassPathResource("/data/customers.xml"));
		reader.setFragmentRootElementName("customer");
		reader.setUnmarshaller(unmarshaller);
		
		return reader;
	}
	
	// Writer
	@Bean
	public ItemWriter<Customer> customerItemWriter() {
		
		return items -> {
			for(Customer item : items) {
				System.out.println(item.toString());
			}
		};
	}
	
	@Bean
	public Step step1E() {
		
		return stepBuilderFactory.get("step1E").<Customer, Customer> chunk(1)
						.faultTolerant()
						.listener(new ChunkListener())
						.reader(xmlItemReader(null))
						.writer(customerItemWriter())
						.build();
	}
	
	@Bean
	public Job readXMLJob(JavaMailSender javaMailSender) {
		
		return jobBuilderFactory.get("readXMLJob").start(step1E())
						.listener(new JobListener(javaMailSender))
						.build();
	}
	
}
