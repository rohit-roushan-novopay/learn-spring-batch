package com.spring.batch_microservice.conf;

import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.spring.batch_microservice.custom.ColumnRangePartitioner;
import com.spring.batch_microservice.custom.CustomerRowMapper;
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
	
	@Bean
	public JdbcPagingItemReader<Customer> pagingItemReader2() {
		
		JdbcPagingItemReader<Customer> reader = new JdbcPagingItemReader<>();
		
		reader.setDataSource(dataSource);
		
		// set the fetch size same as chunk size
		reader.setFetchSize(100);
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
