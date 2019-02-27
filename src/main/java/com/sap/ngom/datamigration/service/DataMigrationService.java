package com.sap.ngom.datamigration.service;

import com.sap.ngom.datamigration.configuration.BatchJobParameterHolder;
import com.sap.ngom.datamigration.exception.RunJobException;
import com.sap.ngom.datamigration.exception.SourceTableNotDefinedException;
import com.sap.ngom.datamigration.listener.BPStepListener;
import com.sap.ngom.datamigration.listener.JobCompletionNotificationListener;
import com.sap.ngom.datamigration.model.JobStatus;
import com.sap.ngom.datamigration.model.MigrateRecord;
import com.sap.ngom.datamigration.processor.CustomItemProcessor;
import com.sap.ngom.datamigration.util.DBConfigReader;
import com.sap.ngom.datamigration.util.TenantHelper;
import com.sap.ngom.datamigration.writer.GenericItemWriter;
import com.sap.ngom.datamigration.writer.SpecificRecordItemWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.SimpleJob;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.task.TaskExecutor;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.ColumnMapRowMapper;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.util.*;

@Service
public class DataMigrationService {
    private static final Logger log = LoggerFactory.getLogger(DataMigrationService.class);

    private static final int SKIP_LIMIT = 10;
    public static final int CHUNK_SIZE = 500;

    @Autowired
    private SimpleJobLauncher jobLauncher;

    @Autowired
    @Qualifier("sourceDataSource")
    private DataSource dataSource;

    @Autowired
    @Qualifier("targetDataSource")
    private DataSource detinationDataSource;

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private JobCompletionNotificationListener jobCompletionNotificationListener;

    @Autowired
    TenantHelper tenantHelper;

    @Autowired
    JobRepository jobRepository;

    @Autowired
    private DBConfigReader dbConfigReader;

    @Autowired
    private TaskExecutor simpleAsyncTaskExecutor;

    @Autowired
    private BatchJobParameterHolder batchJobParameterHolder;

    private static String JOB_NAME_SUFFIX = "_MigrationJob";

    @PostConstruct
    void postConstruct() {
        jobLauncher.setTaskExecutor(simpleAsyncTaskExecutor);
    }

    public void triggerOneMigrationJob(String tableName) {
        tableNameValidation(tableName);

        String jobName = tableName + JOB_NAME_SUFFIX;
        List<Step> stepList = new ArrayList<Step>();
        List<String> tenants = tenantHelper.getAllTenants(tableName);
        if (!tenants.isEmpty()) {
            for (String tenant : tenants) {
                Step step = createOneStep(tenant, tableName);
                stepList.add(step);
            }

            SimpleJob migrationJob = (SimpleJob) jobBuilderFactory.get(jobName)
                    .incrementer(new RunIdIncrementer())
                    .listener(jobCompletionNotificationListener).start(stepList.get(0))
                    .build();
            migrationJob.setSteps(stepList);

            try {
                jobLauncher.run(migrationJob, getJobParameters(tableName));
            } catch (JobExecutionAlreadyRunningException e) {
                throw new RunJobException(e.getMessage());
            } catch (JobRestartException e) {
                throw new RunJobException(e.getMessage());
            } catch (JobInstanceAlreadyCompleteException e) {
                throw new RunJobException(e.getMessage());
            } catch (JobParametersInvalidException e) {
                throw new RunJobException(e.getMessage());
            }
        }

    }

    private JobParameters getJobParameters(String tableName){
        JobParametersBuilder jobBuilder = new JobParametersBuilder();
        jobBuilder.addString(tableName,batchJobParameterHolder.getIncreasingParameter(tableName).toString());
        return jobBuilder.toJobParameters();
    }

    private JobParameters getCurrentRunntingJobParameters(String tableName){
        JobParametersBuilder jobBuilder = new JobParametersBuilder();
        jobBuilder.addString(tableName,batchJobParameterHolder.getCurrentParameter(tableName).toString());
        return jobBuilder.toJobParameters();
    }

    public boolean isJobRunningOnTable(String tableName){
        return batchJobParameterHolder.acquireJobLock(tableName);
    }

    private void tableNameValidation(String tableName) {
        if (!dbConfigReader.getSourceTableNames().contains(tableName)) {
            throw new SourceTableNotDefinedException("There is no table:" + tableName + " in the database");
        }
    }

    private Step createOneStep(String tenant, String table) {
        String targetNameSpace = dbConfigReader.getTargetNameSpace();

        Step tenantSpecificStep = stepBuilderFactory.get(table + "_" + tenant + "_" + "MigrationStep")
                .listener(new BPStepListener(tenant))
                .<Map<String, Object>, Map<String, Object>>chunk(CHUNK_SIZE).faultTolerant().noSkip(Exception.class)
                .skipLimit(SKIP_LIMIT)
                .reader(buildItemReader(dataSource, table, tenant))
                .processor(new CustomItemProcessor())
                .writer(buildItemWriter(detinationDataSource, table, targetNameSpace)).faultTolerant()
                .noSkip(Exception.class).skipLimit(SKIP_LIMIT)
                .build();

        return tenantSpecificStep;
    }

    private Step createOneStepByPrimaryKey(String table, String tenant, String primaryKeyName, String primaryKeyValue) {
        String targetNameSpace = dbConfigReader.getTargetNameSpace();

        Step tenantSpecificStep = stepBuilderFactory.get(table + "_" + primaryKeyValue + "_" + "MigrationStep")
                .listener(new BPStepListener(tenant))
                .<Map<String, Object>, Map<String, Object>>chunk(CHUNK_SIZE).faultTolerant()
                .skip(DuplicateKeyException.class).skipLimit(Integer.MAX_VALUE)
                .reader(buildOneRecordItemReader(dataSource, table, primaryKeyName, primaryKeyValue))
                .processor(new CustomItemProcessor())
                .writer(new SpecificRecordItemWriter(detinationDataSource, table, targetNameSpace)).faultTolerant()
                .skip(DuplicateKeyException.class).skipLimit(Integer.MAX_VALUE)
                .build();

        return tenantSpecificStep;
    }

    private JdbcCursorItemReader<Map<String, Object>> buildItemReader(final DataSource dataSource, String tableName,
            String tenant) {
        JdbcCursorItemReader<Map<String, Object>> itemReader = new JdbcCursorItemReader<>();
        itemReader.setDataSource(dataSource);
        itemReader.setSql("select * from " + tableName + " where tenant_id ='" + tenant + "'");
        itemReader.setRowMapper(new ColumnMapRowMapper());
        return itemReader;
    }

    private JdbcCursorItemReader<Map<String, Object>> buildOneRecordItemReader(final DataSource dataSource,
            String tableName,
            String primaryKeyName,
            String primaryKey) {
        JdbcCursorItemReader<Map<String, Object>> itemReader = new JdbcCursorItemReader<>();
        itemReader.setDataSource(dataSource);
        itemReader.setSql("select * from " + tableName + " where " + primaryKeyName + "='" + primaryKey + "'");
        itemReader.setRowMapper(new ColumnMapRowMapper());
        return itemReader;
    }

    private ItemWriter<Map<String, Object>> buildItemWriter(final DataSource dataSource, final String tableName,
            final String targetNameSpace) {
        // insert into hana
        GenericItemWriter genericItemWriter = new GenericItemWriter(dataSource, tableName, targetNameSpace);
        return genericItemWriter;
    }

    private static JobParameters generateJobParams() {
        return new JobParametersBuilder().addDate("date", new Date()).toJobParameters();
    }

    public JobStatus getJobStatus(String tableName) {
        tableNameValidation(tableName);

        String jobStatus = getLastExecutionStatus(tableName);
        return JobStatus.builder().table(tableName).jobStatus(jobStatus).build();
    }

    private String getLastExecutionStatus (String tableName){
        String executionStatus;
        JobExecution jobExecution = jobRepository.getLastJobExecution(tableName+JOB_NAME_SUFFIX, getCurrentRunntingJobParameters(tableName));
        if(null == jobExecution){
            executionStatus = "No Job Triggered Yet!";
        }else{
            executionStatus = jobExecution.getStatus().toString();
        }
        return executionStatus;
    }

    public List<JobStatus> getAllJobsStatus (){
        List<String> sourceTables = dbConfigReader.getSourceTableNames();
        List<JobStatus> jobStatuses = new ArrayList<>();
        if(sourceTables.isEmpty()){
            return jobStatuses;
        }
        for (String sourceTable : sourceTables) {
            String executionStatus = getLastExecutionStatus(sourceTable);
            JobStatus jobStatus = JobStatus.builder().table(sourceTable).jobStatus(executionStatus).build();
            jobStatuses.add(jobStatus);
        }
        return jobStatuses;
    }
    
    public Set<String> triggerAllMigrationJobs() {
        Set<String> alreadyTriggeredTables = new HashSet<>();
        for(String tableName:dbConfigReader.getSourceTableNames()){
            if(isJobRunningOnTable(tableName)){
                alreadyTriggeredTables.add(tableName);
                log.info("Migration job of table: " + tableName + "skipped from trigger all jobs.");
            }else{
                triggerOneMigrationJob(tableName);
                log.info("Migration job of table: " + tableName + "triggered from trigger all jobs.");
            }
        }
        return alreadyTriggeredTables;
    }

    public void migrateSpecificRecords(List<MigrateRecord> migrateRecords) {

        List<Step> stepList = new ArrayList<Step>();
        String jobName = "specificRecords" + JOB_NAME_SUFFIX;

        for (MigrateRecord migrateRecord : migrateRecords) {
            Step step = createOneStepByPrimaryKey(migrateRecord.tableName, migrateRecord.tenant,
                    migrateRecord.primaryKeyName, migrateRecord.primaryKeyValue);
            stepList.add(step);
        }

        SimpleJob migrationJob = (SimpleJob) jobBuilderFactory.get(jobName)
                .incrementer(new RunIdIncrementer())
                .listener(jobCompletionNotificationListener).start(stepList.get(0))
                .build();

        migrationJob.setSteps(stepList);

        try {
            jobLauncher.run(migrationJob, generateJobParams());
        } catch (JobExecutionAlreadyRunningException e) {
            throw new RunJobException(e.getMessage());
        } catch (JobRestartException e) {
            throw new RunJobException(e.getMessage());
        } catch (JobInstanceAlreadyCompleteException e) {
            throw new RunJobException(e.getMessage());
        } catch (JobParametersInvalidException e) {
            throw new RunJobException(e.getMessage());
        }

    }
}
