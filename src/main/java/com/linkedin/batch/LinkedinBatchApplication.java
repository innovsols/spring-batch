package com.linkedin.batch;

import java.time.LocalDateTime;
import java.util.Date;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.batch.item.support.builder.CompositeItemProcessorBuilder;
import org.springframework.batch.item.validator.BeanValidatingItemProcessor;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@SpringBootApplication
@EnableBatchProcessing
@EnableScheduling
public class LinkedinBatchApplication {
	
	public static String[] names = new String[] {"orderId", "firstName", "lastName", "email", "cost", "itemId","itemName", "shipDate"};
	
	public static String ORDER_SQL = "select order_id, first_name, last_name, "
			+ "email, cost, item_id, item_name, ship_date "
			+ "from SHIPPED_ORDER order by order_id";
	
//	public static String INSERT_ORDER_SQL = "insert into "
//			+ "SHIPPED_ORDER_OUTPUT(order_id, first_name, last_name, email, item_id, item_name, cost, ship_date)"
//			+ " values(:orderId, :firstName, :lastName, :email, :itemId, :itemName, :cost, :shipDate)";
	
	public static String INSERT_ORDER_SQL = "insert into "
			+ "TRACKED_ORDER(order_id, first_name, last_name, email, item_id, item_name, cost, ship_date, tracking_number, free_shipping)"
			+ " values(:orderId,:firstName,:lastName,:email,:itemId,:itemName,:cost,:shipDate,:trackingNumber, :freeShipping)";
	@Autowired
	public JobBuilderFactory jobBuilderFactory;
	
	@Autowired
	public StepBuilderFactory stepBuilderFactory;
	
	@Autowired
	public DataSource dataSource;
	
	@Autowired
	public JobLauncher jobLauncher;
	
	@Scheduled(cron = "0/30 * * * * *")
	public void runJob() throws JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException, JobParametersInvalidException, Exception {
		JobParametersBuilder parametersBuilder = new JobParametersBuilder();
		parametersBuilder.addDate("runTime", new Date());
		this.jobLauncher.run(job(), parametersBuilder.toJobParameters());
	}
	
	@Bean
	public Step step() throws Exception {
		return this.stepBuilderFactory.get("step").tasklet(new Tasklet() {
			
			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				System.out.println("The run time is: " + LocalDateTime.now());
				return RepeatStatus.FINISHED;
			}
		}).build();

	}
	
	@Bean
	public PagingQueryProvider queryProvider() throws Exception {
		SqlPagingQueryProviderFactoryBean factory = new SqlPagingQueryProviderFactoryBean();
		
		factory.setSelectClause("select order_id, first_name, last_name, email, cost, item_id, item_name, ship_date");
		factory.setFromClause("from SHIPPED_ORDER");
		factory.setSortKey("order_id");
		factory.setDataSource(dataSource);
		return factory.getObject();
	}
	
	@Bean
	public ItemReader<Order> itemReader() throws Exception{
	
		
		return new JdbcPagingItemReaderBuilder<Order>()
				.dataSource(dataSource)
				.name("jdbcCursorItemReader")
				.queryProvider(queryProvider())
				.rowMapper(new OrderRowMapper())
				.build();
	}

	@Bean
	public ItemProcessor<Order, Order> orderValidatingItemProcessor() {
		BeanValidatingItemProcessor<Order> itemProcessor = new BeanValidatingItemProcessor<Order>();
		itemProcessor.setFilter(true);
		
		return itemProcessor;
	}
	
	@Bean
	public ItemProcessor<Order, TrackedOrder> trackedOrderItemProcessor() {
	return new TrackedOrderItemProcessor();
	}
	@Bean
	public ItemProcessor<TrackedOrder, TrackedOrder> freeShippingItemProcessor() {
	return new FreeShippingItemProcessor();
	}
	@Bean
	public ItemProcessor< Order, TrackedOrder> compositeItemProcessor() {
		
		return new CompositeItemProcessorBuilder<Order, TrackedOrder>()
					.delegates(orderValidatingItemProcessor(), trackedOrderItemProcessor(), freeShippingItemProcessor())
					.build();
	}
	
	@Bean
	public ItemWriter<TrackedOrder> itemWriter() {
		
//		return new JsonFileItemWriterBuilder<TrackedOrder>()
//				.jsonObjectMarshaller(new JacksonJsonObjectMarshaller<TrackedOrder>())
//				.resource(new FileSystemResource("C:\\Gorakh\\Workspaces\\eclipse-workspace\\data\\output\\order_output.json"))
//				.name("jsonItemWriter")
//				.build();
		
		//Multi-threaded implementation of ItemWriter 
		return new JdbcBatchItemWriterBuilder<TrackedOrder>()
				.dataSource(dataSource)
				.sql(INSERT_ORDER_SQL)
				.beanMapped()
				.build();
	
	}
	
	// Returns the task executor
	@Bean
	public TaskExecutor taskExecutor() {
		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		executor.setCorePoolSize(2);
		executor.setMaxPoolSize(10);
		return executor;
	}

	@Bean
	public Step chunkBasedStep() throws Exception {
		return this.stepBuilderFactory.get("chunkBasedStep")
				.<Order,TrackedOrder>chunk(10)
				.reader(itemReader())
				.processor(compositeItemProcessor())
				.faultTolerant()
				.retry(OrderProcessingException.class)
				.retryLimit(3)
				.listener(new CustomRetryListener())
				.writer(itemWriter())
				.taskExecutor(taskExecutor())
				.build();
	}



	@Bean
	public Job job() throws Exception {
		return this.jobBuilderFactory.get("job")
				.start(step())
				.build();
	}

	public static void main(String[] args) {
		SpringApplication.run(LinkedinBatchApplication.class, args);
	}

}