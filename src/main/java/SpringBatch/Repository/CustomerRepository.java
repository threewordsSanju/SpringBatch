package SpringBatch.Repository;

import org.springframework.data.jpa.repository.JpaRepository;

import SpringBatch.Entity.Customer;

public interface CustomerRepository extends JpaRepository<Customer, Integer> {

}
