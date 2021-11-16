package com.linkedin.batch;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.info.ProjectInfoProperties.Build;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
@EnableBatchProcessing
public class LinkedinBatchApplication {
	
	@Autowired
	public JobBuilderFactory jobBuilderFactory;
	
	@Autowired
	public StepBuilderFactory sBuilderFactory;
	
	@Bean
	public Job deliverPackageJob() {
		return this.jobBuilderFactory.get("deliverPackageJob")
					.start(packageItemStep())
					.next(driveToAddressStep())
					.on("FAILED").to(storePackageStep())
					.from(driveToAddressStep())
					.on("*").to(givePackageToCustomerStep())
					.end()
					.build();
	}
	
	@Bean
	public Step storePackageStep() {
		return this.sBuilderFactory.get("storePackageStep").tasklet(new Tasklet() {
			
			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				System.out.println("Stroing the package while customer address is located");
				return RepeatStatus.FINISHED;
			}
		}).build();
	}
	@Bean
	public Step givePackageToCustomerStep() {
		return this.sBuilderFactory.get("givePackageToCustomerStep").tasklet(new Tasklet() {
			
			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				System.out.println("We have given package to customer");
				return RepeatStatus.FINISHED;
			}
		}).build();
	}
	
	@Bean
	public Step driveToAddressStep() {
		
		boolean GOT_LOST = true;
		return this.sBuilderFactory.get("driveToAddressStep").tasklet(new Tasklet() {
			
			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				if(GOT_LOST) {
					throw new RuntimeException("Got Lost driving to address");
				}
				System.out.println("Successfully arrived at Address");
				return RepeatStatus.FINISHED;
			}
		}).build();
	}
	
	@Bean
	public Step packageItemStep() {
		// TODO Auto-generated method stub
		return this.sBuilderFactory.get("packageItemStep").tasklet(new Tasklet() {
			
			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				String item = chunkContext.getStepContext().getJobParameters().get("item").toString();
				String date = chunkContext.getStepContext().getJobParameters().get("run.date").toString();
				System.out.println(String.format("The item %s has been packaged on %s",item,date));
				return RepeatStatus.FINISHED;
			}
		}).build();
	}

	public static void main(String[] args) {
		SpringApplication.run(LinkedinBatchApplication.class, args);
	}

}
