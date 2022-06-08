package com.spring_poc.spring_batch.conf;

import java.util.List;

import org.springframework.batch.item.ItemWriter;

public class SysoutItemWriter implements ItemWriter<String>{

	@Override
	public void write(List<? extends String> items) throws Exception {
		
		System.out.println("The number of items in this chunk are: " + items.size());
		
		for(String item : items) System.out.println(">> "+item);
	}

}
