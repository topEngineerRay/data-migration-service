package com.sap.ngom.datamigration.configuration;

import com.sap.ngom.datamigration.listener.BPStepListener;
import com.sap.ngom.datamigration.listener.JobCompletionNotificationListener;
import com.sap.ngom.datamigration.mapper.RowMapper.MapItemSqlParameterSourceProvider;
import com.sap.ngom.datamigration.processor.CustomItemProcessor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.configuration.support.JobRegistryBeanPostProcessor;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ResourceLoader;
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


    // job
    @Bean
    public Job bpMigrationJob(JobCompletionNotificationListener listener,Step bpTableMigrationStep) {
        return jobBuilderFactory.get("bpMigrationJob")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .start(bpTableMigrationStep)
                .build();
    }

    //step
    @Bean
    public Step bpTableMigrationStep() {
        return stepBuilderFactory.get("BPTableMigrationStep")
                .transactionManager(dataSourceTransactionManager())
                .listener(new BPStepListener("revcdevkp"))
                .<Map<String,Object>,Map<String,Object>>chunk(10)
                .reader(BPItemReaderPaging(dataSource)).faultTolerant().noSkip(Exception.class).skipLimit(SKIP_LIMIT)
                .processor(BPprocessor())
                .writer(BPItemwriter(detinationDataSource)).faultTolerant().noSkip(Exception.class).skipLimit(10)

                .build();
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
    //writer
    @Bean
    public ItemWriter<Map<String,Object>> BPItemwriter(final DataSource dataSource) {
        String QUERY_INSERT_BP = "INSERT INTO \"com.sap.ngom.db::BusinessPartner.bp\" (PKID, BPID, FIRSTNAME, LASTNAME,BPROLE,COMPANY)"
                + " VALUES (:pkid,:bpid,:firstname, :lastname,:bprole,:company)";
        // insert into hana

        return new JdbcBatchItemWriterBuilder<Map<String,Object>>().itemSqlParameterSourceProvider(new MapItemSqlParameterSourceProvider())
                //.beanMapped()
               .columnMapped()
               .dataSource(dataSource)
                .sql(QUERY_INSERT_BP)
                //.sql("INSERT INTO BP (first_name, last_name) VALUES (:firstName, :lastName)")
                .build();
    }


    @Bean
    public JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor(JobRegistry jobRegistry) {
        JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor = new JobRegistryBeanPostProcessor();
        jobRegistryBeanPostProcessor.setJobRegistry(jobRegistry);
        return jobRegistryBeanPostProcessor;
    }
}
