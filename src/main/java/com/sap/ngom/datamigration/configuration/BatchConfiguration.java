package com.sap.ngom.datamigration.configuration;

import com.sap.ngom.datamigration.processor.CustomItemProcessor;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.configuration.support.JobRegistryBeanPostProcessor;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.core.ColumnMapRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import javax.sql.DataSource;
import java.util.Map;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {
    //define how many record fail will fail a job
    private static final int SKIP_LIMIT = 10;
    @Autowired
    @Qualifier("sourceDataSource")
    private DataSource dataSource;

    @Autowired
    @Qualifier("targetDataSource")
    DataSource detinationDataSource;

    @Autowired
    private JobRepository jobRepository;

    public JobBuilderFactory jobBuilderFactory;

    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    public BatchConfiguration(final JobBuilderFactory jobBuilderFactory,
            final StepBuilderFactory stepBuilderFactory,
            final ResourceLoader resourceLoader) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;

    }

    @Bean
    public JdbcTemplate jdbcTemplateDesinationDB()
    {
        return new JdbcTemplate(detinationDataSource);
    }

    @Bean
    public DataSourceTransactionManager dataSourceTransactionManager() {
        return new DataSourceTransactionManager(detinationDataSource);
    }

    @Bean
    @StepScope
    public JdbcPagingItemReader<Map<String, Object>> BPItemReaderPaging(final DataSource dataSource){

        JdbcPagingItemReader<Map<String, Object>>  itemReader = new JdbcPagingItemReader<>();

        SqlPagingQueryProviderFactoryBean provider = new SqlPagingQueryProviderFactoryBean();

        provider.setDataSource(dataSource);
        provider.setSelectClause("select *");
        provider.setFromClause("from bp");
        provider.setWhereClause("where tenant_id='revcdevkp'");
        provider.setSortKey("bpid");
        try {
            itemReader.setQueryProvider(provider.getObject());
        } catch (Exception e) {
            e.printStackTrace();
        }
        itemReader.setPageSize(100);
        itemReader.setDataSource(dataSource);
        itemReader.setRowMapper(new ColumnMapRowMapper());
        return itemReader;
    }

    //processor
    @Bean
    public ItemProcessor<Map<String,Object>, Map<String,Object>> BPprocessor() {
        return new CustomItemProcessor();
    }

    @Bean
    public JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor(JobRegistry jobRegistry) {
        JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor = new JobRegistryBeanPostProcessor();
        jobRegistryBeanPostProcessor.setJobRegistry(jobRegistry);
        return jobRegistryBeanPostProcessor;
    }

    @Bean
    public SimpleJobLauncher simpleJobLauncher(){
        SimpleJobLauncher simpleJobLauncher = new SimpleJobLauncher();
        simpleJobLauncher.setJobRepository(jobRepository);
        return simpleJobLauncher;
    }

    @Bean
    public TaskExecutor taskExecutor(){
        return new SimpleAsyncTaskExecutor("MigrationService");
    }

    @Bean
    public JdbcTemplate sourcJdbcTemplate(){
        return new JdbcTemplate(dataSource);
    }
}
