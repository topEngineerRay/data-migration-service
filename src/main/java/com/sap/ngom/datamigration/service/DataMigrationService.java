package com.sap.ngom.datamigration.service;

import com.sap.ngom.datamigration.configuration.MyItemWriter;
import com.sap.ngom.datamigration.listener.BPStepListener;
import com.sap.ngom.datamigration.listener.JobCompletionNotificationListener;
import com.sap.ngom.datamigration.mapper.RowMapper.MapItemSqlParameterSourceProvider;
import com.sap.ngom.datamigration.model.JobResult;
import com.sap.ngom.datamigration.processor.BPItemProcessor;
import com.sap.ngom.datamigration.util.DataMigrationServiceUtil;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.DuplicateJobException;
import org.springframework.batch.core.configuration.JobFactory;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.SimpleJob;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.ColumnMapRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

@Service
public class DataMigrationService {

    @Autowired
    JobLauncher jobLauncher;

    @Autowired
    JobRegistry jobRegistry;

    private static final int SKIP_LIMIT = 10;

    @Autowired
    @Qualifier("sourceDataSource")
    private DataSource dataSource;

    @Autowired
    @Qualifier("MTRoutingDataSource")
    DataSource detinationDataSource;

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    JobCompletionNotificationListener jobCompletionNotificationListener;

    @Autowired
    DataMigrationServiceUtil dataMigrationServiceUtil;

    JobExecution bpMigrationjobExecution = null;
    JobExecution addressMigrationJobExecution = null;
    JobExecution bprelationshipMigrationJobExecution = null;
    JobExecution customreferenceMigrationJobExecution = null;
    JobExecution externalinfoMigrationJobExecution = null;
    JobExecution marketMigrationJobExecution = null;
    JobExecution objectreplicationstatusMigrationJobExecution = null;

    public void triggerBpMigration(String serviceName) {
        //run all job
        try {
            Job bpMigrationjob = jobRegistry.getJob("bpMigrationJob");
            Job addressMigrationJob = jobRegistry.getJob("addressMigrationJob");
            Job bprelationshipMigrationJob = jobRegistry.getJob("bprelationshipMigrationJob");
            Job customreferenceMigrationJob = jobRegistry.getJob("customreferenceMigrationJob");
            Job externalinfoMigrationJob = jobRegistry.getJob("externalinfoMigrationJob");
            Job marketMigrationJob = jobRegistry.getJob("marketMigrationJob");
            Job objectreplicationstatusMigrationJob = jobRegistry.getJob("objectreplicationstatusMigrationJob");

            bpMigrationjobExecution = jobLauncher.run(bpMigrationjob, generateJobParams());
            addressMigrationJobExecution = jobLauncher.run(addressMigrationJob, generateJobParams());
            bprelationshipMigrationJobExecution = jobLauncher.run(bprelationshipMigrationJob, generateJobParams());
            customreferenceMigrationJobExecution = jobLauncher.run(customreferenceMigrationJob, generateJobParams());
            externalinfoMigrationJobExecution = jobLauncher.run(externalinfoMigrationJob, generateJobParams());
            marketMigrationJobExecution = jobLauncher.run(marketMigrationJob, generateJobParams());
            objectreplicationstatusMigrationJobExecution = jobLauncher
                    .run(objectreplicationstatusMigrationJob, generateJobParams());

        } catch (NoSuchJobException e) {
            e.printStackTrace();
        } catch (JobExecutionAlreadyRunningException e) {
            e.printStackTrace();
        } catch (JobRestartException e) {
            e.printStackTrace();
        } catch (JobInstanceAlreadyCompleteException e) {
            e.printStackTrace();
        } catch (JobParametersInvalidException e) {
            e.printStackTrace();
        }

    }

    public void triggerOneMigrationJob(String tableName) {
        //we have two different type of jobs: FlowJob and SimpleJob

        try {
            String jobName = tableName + "_" + "MigrationJobDynamic";
            List<Step> stepList = new ArrayList<Step>();
            List<String> tenants = dataMigrationServiceUtil.getAllTenants(tableName, dataSource);
            if (!tenants.isEmpty()) {
                Step step = null;
                //notice: For test purpose we'd better not migarate all tenants,
                // or else there would be a lot of tenants,below code only migrate two tenants
                for (int i = 0; i < 2; i++) {
                    step = createOneStep(tenants.get(i), tableName);
                    stepList.add(step);
                }
                //below code will migrate all tenants
                    /*  for (String tenant : tenants) {
                        step = createOneStep(tenant);
                        stepList.add(step);
                    }*/

                SimpleJob migrationJob = (SimpleJob) jobBuilderFactory.get(jobName)
                        .incrementer(new RunIdIncrementer())
                        .listener(jobCompletionNotificationListener).start(stepList.get(0))
                        .build();
                migrationJob.setSteps(stepList);
                jobLauncher.run(migrationJob, generateJobParams());
            }

        } catch (JobExecutionAlreadyRunningException e) {
            e.printStackTrace();
        } catch (JobRestartException e) {
            e.printStackTrace();
        } catch (JobInstanceAlreadyCompleteException e) {
            e.printStackTrace();
        } catch (JobParametersInvalidException e) {
            e.printStackTrace();
        }
    }

    private Step createOneStep(String tenant, String table) {
        //not skip any exceptions now
        Step tenantSpecificStep = stepBuilderFactory.get(table + "_" + tenant + "_" + "MigrationStepDynamic")
                .listener(new BPStepListener(tenant))
                .<Map<String, Object>, Map<String, Object>>chunk(10).faultTolerant().noSkip(Exception.class).skipLimit(SKIP_LIMIT)
                .reader(itemReader(dataSource, tenant))
                .processor(new BPItemProcessor())
                .writer(myItemwriter(detinationDataSource, table)).faultTolerant().noSkip(Exception.class).skipLimit(SKIP_LIMIT)
                .build();

        return tenantSpecificStep;
    }



    public JdbcCursorItemReader<Map<String, Object>> itemReader(final DataSource dataSource, String tenant) {
        //get bp table data from postgresql
        JdbcCursorItemReader<Map<String, Object>> itemReader = new JdbcCursorItemReader<>();
        itemReader.setDataSource(dataSource);
        itemReader.setSql("select * from bp b where b.tenant_id ='" + tenant + "'");
        itemReader.setRowMapper(new ColumnMapRowMapper());
        return itemReader;
    }

    public ItemWriter<Map<String, Object>> myItemwriter(final DataSource dataSource, final String tableName) {

        // insert into hana
        MyItemWriter myItemWriter = new MyItemWriter(dataSource, tableName);
        return myItemWriter;
    }

    public void migrationFailedRecordRetry(String tableName, String pkid) {

    }

    private static JobParameters generateJobParams() {
        return new JobParametersBuilder().addDate("date", new Date()).toJobParameters();
    }

    public List<BatchStatus> getAllJobsStatus(String serviceName) {
        List<BatchStatus> statusList = new ArrayList<>();
        if (null != bpMigrationjobExecution)
            statusList.add(bpMigrationjobExecution.getStatus());

        if (null != addressMigrationJobExecution)
            statusList.add(addressMigrationJobExecution.getStatus());

        if (null != bprelationshipMigrationJobExecution)
            statusList.add(bprelationshipMigrationJobExecution.getStatus());

        if (null != customreferenceMigrationJobExecution)
            statusList.add(customreferenceMigrationJobExecution.getStatus());

        if (null != externalinfoMigrationJobExecution)
            statusList.add(externalinfoMigrationJobExecution.getStatus());

        if (null != marketMigrationJobExecution)
            statusList.add(marketMigrationJobExecution.getStatus());

        if (null != objectreplicationstatusMigrationJobExecution)
            statusList.add(objectreplicationstatusMigrationJobExecution.getStatus());

        return statusList;
    }

    public JobResult getJobsStatus(String jobName) {

        return null;
    }
}
