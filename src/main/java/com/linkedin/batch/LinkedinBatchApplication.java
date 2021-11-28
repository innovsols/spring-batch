package com.linkedin.batch;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.FileSystemResource;

@SpringBootApplication
@EnableBatchProcessing
public class LinkedinBatchApplication {
	
	public static String[] names = new String[] {"orderId", "firstName", "lastName", "email", "cost", "itemId","itemName", "shipDate"};
	
	public static String ORDER_SQL = "select order_id, first_name, last_name, "
			+ "email, cost, item_id, item_name, ship_date "
			+ "from SHIPPED_ORDER order by order_id";
	
	@Autowired
	public JobBuilderFactory jobBuilderFactory;
	
	@Autowired
	public StepBuilderFactory stepBuilderFactory;
	
	@Autowired
	public DataSource dataSource;
	

//	public ItemReader<String> itemReader(){
//		return new SimpleItemReader();
//	}
	
//	@Bean
//	public PagingQueryProvider queryProvider() throws Exception {
//		SqlPagingQueryProviderFactoryBean factoryBean = new SqlPagingQueryProviderFactoryBean();
//		
//		factoryBean.setSelectClause("select order_id, first_name, last_name, email, cost, item_id, item_name, ship_date ");
//		factoryBean.setFromClause("from SHIPPED_ORDER");
//		factoryBean.setSortKey("order_id");
//		factoryBean.setDataSource(dataSource);
//		return factoryBean.getObject();
//	}
	
	public ItemReader<Order> itemReader() throws Exception{
	
		
		return new JdbcCursorItemReaderBuilder<Order>()
				.dataSource(dataSource)
				.name("jdbcCursorItemReader")
				.sql(ORDER_SQL)
				.rowMapper(new OrderRowMapper())
				.build();
	}

	@Bean
	public ItemWriter<Order> itemWriter() {
		FlatFileItemWriter<Order> itemWriter = new FlatFileItemWriter<Order>();
		itemWriter.setResource(new FileSystemResource("C:\\Gorakh\\Workspaces\\eclipse-workspace\\data\\output\\order_output.csv"));
		DelimitedLineAggregator<Order> dLineAggregator = new DelimitedLineAggregator<Order>();
		dLineAggregator.setDelimiter(",");
		
		BeanWrapperFieldExtractor<Order> fieldExtractor = new BeanWrapperFieldExtractor<Order>();
		fieldExtractor.setNames(names);
		
		dLineAggregator.setFieldExtractor(fieldExtractor);
		
		itemWriter.setLineAggregator(dLineAggregator);
		
		return itemWriter;
	}

	@Bean
	public Step chunkBasedStep() throws Exception {
		return this.stepBuilderFactory.get("chunkBasedStep")
				.<Order,Order>chunk(10)
				.reader(itemReader())
				.writer(itemWriter()).build();
	}
	


	@Bean
	public Job job() throws Exception {
		return this.jobBuilderFactory.get("job")
				.start(chunkBasedStep())
				.build();
	}

	public static void main(String[] args) {
		SpringApplication.run(LinkedinBatchApplication.class, args);
	}

}