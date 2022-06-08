package com.spring_poc.spring_batch.conf;

import java.util.Iterator;
import java.util.List;

import org.springframework.batch.item.ItemReader;

public class StatelessItemReader implements ItemReader<String>{

	private final Iterator<String> data;
	
	public StatelessItemReader(List<String> data) {
		this.data = data.iterator();
	}

	/*
	 * returns a single item/record.
     * called over and over within the context of a chunk until null is returned.
     * once null is returned, it indicates that the input has been exhausted.
     * 
     * Item can be anything for eg. a complex java object
	 */
	@Override
	public String read() throws Exception {
		
		if(this.data.hasNext()) return this.data.next();
		else return null;
	}

}
