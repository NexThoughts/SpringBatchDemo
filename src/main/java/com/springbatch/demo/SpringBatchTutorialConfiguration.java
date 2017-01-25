package com.springbatch.demo;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

/**
 * This class contains all the configuration of the Spring Batch application
 * used for this tutorial. It contains readers, writers, processors, jobs, steps
 * and all the needed beans.
 *
 * @author dgutierrez-diez
 */
@Configuration
@EnableBatchProcessing
public class SpringBatchTutorialConfiguration {

    /**
     * Modes, should be injected as parameter TODO
     */
    // private String mode = "custom";

    private String mode = "flat";

    // private String mode = "mongo";

    // private String mode = "flat";

    // private String mode = "hsqldb";

	/* ********************************************
     * READERS This section contains all the readers
	 * ********************************************
	 */

    /**
     * @return a reader
     */

    @Bean
    public ItemReader<CustomPojo> reader() {
        if ("flat".equals(this.mode)) {
            // flat file item reader (using an csv extractor)
            FlatFileItemReader<CustomPojo> reader = new FlatFileItemReader<CustomPojo>();
            reader.setResource(new ClassPathResource("input.csv"));
            reader.setLineMapper(new DefaultLineMapper<CustomPojo>() {
                {
                    setLineTokenizer(new DelimitedLineTokenizer() {
                        {
                            setNames(new String[]{"id", "description"});
                        }
                    });
                    setFieldSetMapper(new BeanWrapperFieldSetMapper<CustomPojo>() {
                        {
                            setTargetType(CustomPojo.class);
                        }
                    });
                }
            });
            return reader;
        } else {
            // custom item reader (dummy), using an iterator within an internal
            // list
            CustomItemReader reader = new CustomItemReader();
            List<CustomPojo> pojos = new ArrayList<CustomPojo>();
            pojos.add(new CustomPojo("1", "desc1"));
            pojos.add(new CustomPojo("2", "desc2"));
            pojos.add(new CustomPojo("3", "desc3"));
            reader.setPojos(pojos);
            reader.setIterator(reader.getPojos().iterator());
            return reader;
        }
    }

    @Bean
    public ItemProcessor<CustomPojo, CustomPojo> processor() {
        return new CustomItemProcessor();
    }

    @Bean
    public ItemWriter<CustomPojo> writer(DataSource dataSource) {
        if ("mysql".equals(this.mode)) {
            JdbcBatchItemWriter<CustomPojo> writer = new JdbcBatchItemWriter<CustomPojo>();

            writer.setSql("INSERT INTO pojo (id, description) VALUES (:id, :description)");
            writer.setDataSource(dataSource);
            writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<CustomPojo>());
            return writer;
        } else if ("flat".equals(this.mode)) {
            FlatFileItemWriter<CustomPojo> writer = new CustomFlatFileItemWriter();
            writer.setResource(new ClassPathResource("output.csv"));

            BeanWrapperFieldExtractor<CustomPojo> fieldExtractor = new CustomFieldExtractor();
            fieldExtractor.setNames(new String[]{"id", "description"});

            DelimitedLineAggregator<CustomPojo> delLineAgg = new CustomDelimitedAggregator();
            delLineAgg.setDelimiter(",");
            delLineAgg.setFieldExtractor(fieldExtractor);

            writer.setLineAggregator(delLineAgg);
            return writer;
        } else {
            CustomItemWriter writer = new CustomItemWriter();
            return writer;
        }
    }

    @Bean
    public Job importUserJob(JobBuilderFactory jobs, Step s1) {
        return jobs.get("importUserJob1").incrementer(new RunIdIncrementer())
                .flow(s1).end().build();
    }

    @Bean
    public Step step1(StepBuilderFactory stepBuilderFactory,
                      ItemReader<CustomPojo> reader, ItemWriter<CustomPojo> writer,
                      ItemProcessor<CustomPojo, CustomPojo> processor) {
        return stepBuilderFactory.get("step1")
                .<CustomPojo, CustomPojo>chunk(10).reader(reader)
                .processor(processor).writer(writer).build();
    }

    @Bean
    public JdbcTemplate jdbcTemplate(DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    @Bean
    public DataSource dataSource() throws SQLException {
        // mysql data source
        final DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource
                .setUrl("jdbc:mysql://localhost/spring_batch");
        dataSource.setUsername("root");
        dataSource.setPassword("nextdefault");
        return dataSource;
    }

//    @Bean
//    public JobLauncherTestUtils jobLauncherTestUtils() {
//        return new JobLauncherTestUtils();
//    }

}