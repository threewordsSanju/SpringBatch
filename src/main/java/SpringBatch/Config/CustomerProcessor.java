package SpringBatch.Config;

import org.springframework.batch.item.ItemProcessor;

import SpringBatch.Entity.Customer;

public class CustomerProcessor implements ItemProcessor<Customer, Customer> {

	@Override
	public Customer process(Customer customer) throws Exception {
	return customer;
	}
}