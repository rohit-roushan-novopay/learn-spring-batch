package com.spring.batch_microservice.custom;

import org.springframework.batch.core.SkipListener;

import com.spring.batch_microservice.pojo.Customer;

/*
 * Called after all the items in the chunk are read/processed/written
 */
public class CustomSkipListener implements SkipListener<Customer, Customer>{

	@Override
	public void onSkipInRead(Throwable t) {
		// since an unsuccessful read doesn't return an item, there's no way to record what data was read
	}

	@Override
	public void onSkipInWrite(Customer item, Throwable t) {
		
		System.out.println("Skipping item " + item + " because of the error: " + t);
	}

	@Override
	public void onSkipInProcess(Customer item, Throwable t) {
		// TODO Auto-generated method stub
		
	}

}
