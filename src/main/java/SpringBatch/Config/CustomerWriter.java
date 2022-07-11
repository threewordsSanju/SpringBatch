package SpringBatch.Config;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import SpringBatch.Entity.Customer;
import SpringBatch.Repository.CustomerRepository;

@Component
public class CustomerWriter implements org.springframework.batch.item.ItemWriter<Customer> {

	@Autowired
	private CustomerRepository customerRepository;

	@Override
	public void write(List<? extends Customer> items) throws Exception {
		System.out.println("Thread Name: -" + Thread.currentThread().getName());
		customerRepository.saveAll(items);
	}

}
