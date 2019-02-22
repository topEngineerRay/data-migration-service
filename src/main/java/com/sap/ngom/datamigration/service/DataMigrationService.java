package com.sap.ngom.datamigration.service;

import com.sap.ngom.datamigration.exception.RunJobException;
import com.sap.ngom.datamigration.exception.SourceTableNotDefinedException;
import com.sap.ngom.datamigration.listener.BPStepListener;
import com.sap.ngom.datamigration.listener.JobCompletionNotificationListener;
import com.sap.ngom.datamigration.model.JobStatus;
import com.sap.ngom.datamigration.processor.CustomItemProcessor;
import com.sap.ngom.datamigration.util.DBConfigReader;
import com.sap.ngom.datamigration.util.TenantHelper;
import com.sap.ngom.datamigration.writer.GenericItemWriter;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.SimpleJob;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.task.TaskExecutor;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.ColumnMapRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

@Service
public class DataMigrationService {

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
    private TenantHelper tenantHelper;

    @Autowired
    private DBConfigReader dbConfigReader;

    @Autowired
    private TaskExecutor simpleAsyncTaskExecutor;

    @Autowired
    @Qualifier("batchDataJDBCTemplate")
    private JdbcTemplate sourcJdbcTemplate;

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
                .<Map<String, Object>, Map<String, Object>>chunk(CHUNK_SIZE).faultTolerant().skip(DuplicateKeyException.class)
                .reader(buildOneRecordItemReader(dataSource, table, primaryKeyName, primaryKeyValue))
                .processor(new CustomItemProcessor())
                .writer(buildItemWriter(detinationDataSource, table, targetNameSpace)).faultTolerant()
                .skip(DuplicateKeyException.class)
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

        String jobName = tableName + JOB_NAME_SUFFIX;
        String jobStatus = getLastExecutionStatus(jobName);
        return JobStatus.builder().table(tableName).jobStatus(jobStatus).build();
    }

    private String getLastExecutionStatus(String jobName) {
        String executionStatus;
        try{
            executionStatus = sourcJdbcTemplate.queryForObject("SELECT STATUS FROM batch_job_execution WHERE job_instance_id IN (SELECT JOB_INSTANCE_ID FROM batch_job_instance WHERE job_name = ?) ORDER BY start_time DESC LIMIT 1",
                new Object[]{jobName}, String.class );
        }catch (EmptyResultDataAccessException exception){
            executionStatus = "No Job Triggered Yet!";
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
            String executionStatus = getLastExecutionStatus(sourceTable + JOB_NAME_SUFFIX);
            JobStatus jobStatus = JobStatus.builder().table(sourceTable).jobStatus(executionStatus).build();
            jobStatuses.add(jobStatus);
        }
        return jobStatuses;
    }
    
    public void triggerAllMigrationJobs() {
        for(String tableName:dbConfigReader.getSourceTableNames()){
            triggerOneMigrationJob(tableName);
        }
    }

    public void migrateSingleRecord(String tableName, String tenant, String primaryKeyName,
            String primaryKeyValue) {

        String jobName = tableName + primaryKeyValue + JOB_NAME_SUFFIX;
        Step step = createOneStepByPrimaryKey(tableName, tenant, primaryKeyName, primaryKeyValue);

        SimpleJob migrationJob = (SimpleJob) jobBuilderFactory.get(jobName)
                .incrementer(new RunIdIncrementer())
                .listener(jobCompletionNotificationListener).start(step)
                .build();
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
