package com.linkedin.batch;

import java.util.UUID;

import org.springframework.batch.item.ItemProcessor;

public class TrackedOrderItemProcessor implements ItemProcessor<Order, TrackedOrder> {

	@Override
	public TrackedOrder process(Order item) throws Exception {
		
		System.out.println("Processing order with id: "+ item.getOrderId());
		System.out.println("Processing with Thread: "+ Thread.currentThread().getName());
		
		TrackedOrder trackedOrder = new TrackedOrder(item);
		trackedOrder.setTrackingNumber(getTrackingNumber());
		return trackedOrder;
	}
	
	private String getTrackingNumber() throws OrderProcessingException {
		
		if(Math.random() < .01) {
			throw new OrderProcessingException();
		}

		return UUID.randomUUID().toString(); 
	}

}
