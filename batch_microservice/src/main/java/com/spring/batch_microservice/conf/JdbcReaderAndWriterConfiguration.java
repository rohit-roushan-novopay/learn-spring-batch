package com.spring.batch_microservice.conf;

import javax.sql.DataSource;

import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.spring.batch_microservice.custom.ColumnRangePartitioner;
import com.spring.batch_microservice.pojo.Customer;

@Configuration
public class JdbcReaderAndWriterConfiguration {

	@Autowired
	private DataSource dataSource;
	
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
	@Bean("itemWriter")
	public JdbcBatchItemWriter<Customer> jdbcItemWriter() {
		
		JdbcBatchItemWriter<Customer> itemWriter = new JdbcBatchItemWriter<>();
		itemWriter.setDataSource(this.dataSource);
		itemWriter.setSql("INSERT INTO NEW_CUSTOMER VALUES (:firstName, :lastName, :birthDate)");
		itemWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
		itemWriter.afterPropertiesSet();
		
		return itemWriter;
	}
}
