package com.linkedin.batch;

import java.util.Iterator;
import java.util.List;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;

public class SimpleItemReader implements ItemReader<String> {
	
	private List<String> dataSet = List.of("1","2","3","4","5");
	
	private Iterator<String> iterator;
	
	public SimpleItemReader() {
		this.iterator = this.dataSet.iterator();
	}

	@Override
	public String read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
		
		return iterator.hasNext() ? iterator.next():null;
	}

}
