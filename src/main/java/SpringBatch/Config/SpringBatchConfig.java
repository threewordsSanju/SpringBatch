package SpringBatch.Config;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import SpringBatch.Entity.Customer;
import SpringBatch.Partation.ColumnRangePartioner;
import lombok.AllArgsConstructor;

@Configuration
@EnableBatchProcessing
@AllArgsConstructor
public class SpringBatchConfig {

	private JobBuilderFactory jobBuilderFactory;

	private StepBuilderFactory stepBuilderFactory;


	private CustomerWriter CustomerWriter;

	// to read the information from source
	@Bean
	public FlatFileItemReader<Customer> reader() {
		FlatFileItemReader<Customer> itemreader = new FlatFileItemReader<Customer>();
		itemreader.setResource(new FileSystemResource("src/main/resources/customers.csv"));
		itemreader.setName("csvReader");
		itemreader.setLinesToSkip(1);
		itemreader.setLineMapper(lineMapper());
		return itemreader;
	}

	private LineMapper<Customer> lineMapper() {
		DefaultLineMapper<Customer> lineMapper = new DefaultLineMapper<>();
		DelimitedLineTokenizer LineTokenizer = new DelimitedLineTokenizer();
		LineTokenizer.setDelimiter(",");
		LineTokenizer.setStrict(false);
		LineTokenizer.setNames("id", "firstName", "lastName", "email", "gender", "contactNo", "country", "dob");

		BeanWrapperFieldSetMapper<Customer> fieldsetMapper = new BeanWrapperFieldSetMapper<Customer>();
		fieldsetMapper.setTargetType(Customer.class);

		lineMapper.setLineTokenizer(LineTokenizer);
		lineMapper.setFieldSetMapper(fieldsetMapper);

		return lineMapper;

	}

	@Bean
	public CustomerProcessor processor() {
		return new CustomerProcessor();
	}
	
	
	@Bean
	public ColumnRangePartioner partioner() {
		return new ColumnRangePartioner();
	}

	@Bean
	public PartitionHandler partitionHandler() {
		TaskExecutorPartitionHandler TaskExecutorPartitionHandler = new TaskExecutorPartitionHandler();
		TaskExecutorPartitionHandler.setGridSize(4);
		TaskExecutorPartitionHandler.setTaskExecutor(taskExecutor());
		TaskExecutorPartitionHandler.setStep(slaveStep());
		return TaskExecutorPartitionHandler;
	}


	@Bean
	public Step slaveStep() {
		return (Step) stepBuilderFactory.get("slaveStep").<Customer, Customer>chunk(250).reader(reader())
				.processor(processor()).writer(CustomerWriter).build();
	}

	@Bean
	public Step masterStep() {
		return stepBuilderFactory.get("masterStep").partitioner(slaveStep().getName(), partioner())
				.partitionHandler(partitionHandler()).build();
	}

	@Bean
	public Job runJob() {
		return (Job) jobBuilderFactory.get("importCustomers").flow(masterStep()).end().build();
	}

	
	@Bean
	public TaskExecutor taskExecutor() {
		ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
		taskExecutor.setMaxPoolSize(4);
		taskExecutor.setCorePoolSize(4);
		taskExecutor.setQueueCapacity(4);
		return taskExecutor;
	}

}