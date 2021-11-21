package com.linkedin.batch;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
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
					.on("*").to(deliveryFlow())
					.end()
					.build();
	}
	
	@Bean
	public JobExecutionDecider decider() {
		return new DeliveryDecider();
	}
	
	@Bean
	public JobExecutionDecider receiptDecider() {
		return new ReceiptDecider();
	}
	
	
	@Bean
	public Step thankCustomerStep() {
		return this.sBuilderFactory.get("thankCustomerStep").tasklet(new Tasklet() {
			
			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				System.out.println("Thank You Customer");
				return RepeatStatus.FINISHED;
			}
		}).build();
	}
	
	@Bean
	public Step refundStep() {
		return this.sBuilderFactory.get("refundStep").tasklet(new Tasklet() {
			
			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				System.out.println("Refunding customer money");
				return RepeatStatus.FINISHED;
			}
		}).build();
	}
	
	@Bean
	public Step leaveAtDoorStep() {
		return this.sBuilderFactory.get("leaveAtDoorStep").tasklet(new Tasklet() {
			
			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				System.out.println("leaving the package at the door");
				return RepeatStatus.FINISHED;
			}
		}).build();
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
		
		boolean GOT_LOST = false;
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
	
	@Bean
	public StepExecutionListener selectFlowerListener() {
		return new FlowerStepExecutionListener();
	}
	
	@Bean
    public Step selectFlowersStep() {
        return this.sBuilderFactory.get("selectFlowersStep").tasklet(new Tasklet() {

            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
                System.out.println("Gathering flowers for order.");
                return RepeatStatus.FINISHED; 
            }
            
        }).listener(selectFlowerListener()).build();
    }

    @Bean
    public Step removeThornsStep() {
        return this.sBuilderFactory.get("removeThornsStep").tasklet(new Tasklet() {

            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
                System.out.println("Remove thorns from roses.");
                return RepeatStatus.FINISHED; 
            }
            
        }).build();
    }
    
    @Bean
    public Step arrangeFlowersStep() {
        return this.sBuilderFactory.get("arrangeFlowersStep").tasklet(new Tasklet() {
        	
            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
                System.out.println("Arranging flowers for order.");
                return RepeatStatus.FINISHED; 
            }
            
        }).build();
    }

    @Bean
    public Job prepareFlowers() {
        return this.jobBuilderFactory.get("prepareFlowersJob")
        		.start(selectFlowersStep())
        			.on("TRIM REQUIRED").to(removeThornsStep()).next(arrangeFlowersStep())
        		.from(selectFlowersStep())
        			.on("NO TRIM REQUIRED").to(arrangeFlowersStep())
        		.from(arrangeFlowersStep()).on("*").to(deliveryFlow())
        		.end()
        		.build();
    }
    
	@Bean
	public Flow deliveryFlow() {
		return new FlowBuilder<SimpleFlow>("deliveryFlow").start(driveToAddressStep())
					.on("FAILED").fail()
				.from(driveToAddressStep())
					.on("*").to(decider())
						.on("PRESENT").to(givePackageToCustomerStep())
							.next(receiptDecider()).on("CORRECT").to(thankCustomerStep())
							.from(receiptDecider()).on("INCORRECT").to(refundStep())
					.from(decider())
						.on("NOT_PRESENT").to(leaveAtDoorStep()).build();

	}

	public static void main(String[] args) {
		SpringApplication.run(LinkedinBatchApplication.class, args);
	}

}
