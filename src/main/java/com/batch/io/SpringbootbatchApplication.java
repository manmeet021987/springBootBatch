package com.batch.io;

import java.util.Scanner;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

import com.batch.io.model.CustomeFileReader;
import com.batch.io.partioner.DummyTasklet;
import com.batch.io.partioner.Partioner;
import com.batch.io.step.Processor;

@EnableBatchProcessing
@SpringBootApplication
public class SpringbootbatchApplication{
	
		 
	@Autowired
	  private JobBuilderFactory jobBuilderFactory;
	  @Autowired
	  private StepBuilderFactory stepBuilderFactory;
	  String path="files/MyFile.txt";
		int noOfThreads=4; 
	  
		 

	public static void main(String[] args) {
		SpringApplication.run(SpringbootbatchApplication.class, args);
		}
	
	
	 
	 
	  @Bean
	  public Job PartitionJob() {
	    return jobBuilderFactory.get("partitionJob").incrementer(new RunIdIncrementer())
	        .start(masterStep()).next(step2()).build();
	  }
	 
	  @Bean
	  public Step step2() {
	    return stepBuilderFactory.get("step2").tasklet(dummyTask()).build();
	  }
	 
	  @Bean
	  public DummyTasklet dummyTask() {
	    return new DummyTasklet();
	  }
	 
	  @Bean
	  public Step masterStep() {
	    return stepBuilderFactory.get("masterStep").partitioner(slave().getName(), rangePartitioner())
	        .partitionHandler(masterSlaveHandler()).build();
	  }
	 
	  //passing noOfThreads to girdSize so that one thread(partition) can be assigned to each line.
	  @Bean
	  public PartitionHandler masterSlaveHandler() {
	    TaskExecutorPartitionHandler handler = new TaskExecutorPartitionHandler();
	    handler.setGridSize(noOfThreads);
	    handler.setTaskExecutor(taskExecutor());
	    handler.setStep(slave());
	    try {
	      handler.afterPropertiesSet();
	    } catch (Exception e) {
	      e.printStackTrace();
	    }
	    return handler;
	  }
	 
	  @Bean(name = "slave")
	  public Step slave() {
	
	 
	    return stepBuilderFactory.get("slave").<CustomeFileReader,CustomeFileReader>chunk(1)
	        .reader(slaveReader(path))
	        .processor(slaveProcessor(null)).writer(slaveWriter()).build();
	  }
	 
	  @Bean
	  public Partioner rangePartitioner() {
	    return new Partioner();
	  }
	 
	  @Bean
	  public SimpleAsyncTaskExecutor taskExecutor() {
	    return new SimpleAsyncTaskExecutor();
	  }
	 
	  @Bean
	  @StepScope
	  public Processor slaveProcessor(@Value("#{stepExecutionContext[name]}") String name) {
	    
	  Processor processor = new Processor();
	  processor.setThreadName(name);
	  
	    return processor;
	  }
	 
	  // Reader to read the file from given path 
	  @Bean
	  @StepScope
	  @Value("files/MyFile.txt")
	  public FlatFileItemReader<CustomeFileReader> slaveReader(String path)
	  {
		 
		  
		  FlatFileItemReader<CustomeFileReader> reader = new FlatFileItemReader<CustomeFileReader>();
		  
		    reader.setResource(new FileSystemResource(path));
		    reader.setLineMapper(new DefaultLineMapper<CustomeFileReader>() {
		      {
		        setLineTokenizer(new DelimitedLineTokenizer() {
		          {
		            setNames(new String[] { "fileline"});
		          }
		        });
		        setFieldSetMapper(new BeanWrapperFieldSetMapper<CustomeFileReader>() {
		          {
		            setTargetType(CustomeFileReader.class);
		          }
		        });
		      }
		    });
		    return reader;
		
			   
		  
	  }
	  //Writer to write the encrypted lines in the file
	  @Bean
	  @StepScope
	  public FlatFileItemWriter<CustomeFileReader> slaveWriter() {
	    FlatFileItemWriter<CustomeFileReader> writer = new FlatFileItemWriter<>();
	   
	    writer.setResource(new FileSystemResource("files/processedfile.txt"));
	    writer.setShouldDeleteIfEmpty(true);
        writer.setShouldDeleteIfExists(true);
	    writer.setAppendAllowed(true);
	    writer.setLineAggregator(new DelimitedLineAggregator<CustomeFileReader>() {{
	      setDelimiter(",");
	      setFieldExtractor(new BeanWrapperFieldExtractor<CustomeFileReader>() {{
	        setNames(new String[]{"fileline"});
	      }});
	    }});
	    return writer;
	  }
	 
	
	     
	
	 
	 

}
