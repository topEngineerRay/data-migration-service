package com.sap.ngom.datamigration.service;

import com.sap.ngom.datamigration.configuration.BatchJobParameterHolder;
import com.sap.ngom.datamigration.exception.JobAlreadyRuningException;
import com.sap.ngom.datamigration.exception.RunJobException;
import com.sap.ngom.datamigration.exception.SourceTableNotDefinedException;
import com.sap.ngom.datamigration.listener.BPStepListener;
import com.sap.ngom.datamigration.listener.JobCompletionNotificationListener;
import com.sap.ngom.datamigration.model.JobStatus;
import com.sap.ngom.datamigration.exception.SourceTableNotDefinedException;
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
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.task.TaskExecutor;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.ColumnMapRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.util.*;

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
    TenantHelper tenantHelper;

    @Autowired
    JobRepository jobRepository;

    @Autowired
    private DBConfigReader dbConfigReader;

    @Autowired
    private TaskExecutor simpleAsyncTaskExecutor;

    @Autowired
    private BatchJobParameterHolder batchJobParameterHolder;

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
        checkExistJobRunning(tableName);

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
        jobBuilder.addString(tableName,batchJobParameterHolder.getParameter(tableName).toString());
        return jobBuilder.toJobParameters();
    }

    private void checkExistJobRunning(String tableName){
        JobExecution jobExecution = jobRepository.getLastJobExecution(tableName+JOB_NAME_SUFFIX, getJobParameters(tableName));
        if(null != jobExecution){
            BatchStatus batchStatus = jobExecution.getStatus();
            if (batchStatus.equals(BatchStatus.STARTED) || batchStatus.equals(BatchStatus.STARTING) || batchStatus.equals(BatchStatus.UNKNOWN)){
                throw new JobAlreadyRuningException("Job can't be executed, currently another job is running for this table");
            }
        }

    }

    private JobParameters getJobParameters(String jobParameter, String jobName) {
        JobParametersBuilder jobBuilder = new JobParametersBuilder();
        jobBuilder.addString(jobName, jobParameter);
        return jobBuilder.toJobParameters();
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

    private JdbcCursorItemReader<Map<String, Object>> buildItemReader(final DataSource dataSource, String tableName,
            String tenant) {
        JdbcCursorItemReader<Map<String, Object>> itemReader = new JdbcCursorItemReader<>();
        itemReader.setDataSource(dataSource);
        itemReader.setSql("select * from " + tableName + " where tenant_id ='" + tenant + "'");
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

    public JobStatus getJobsStatus(String tableName) {
        tableNameValidation(tableName);

        String jobName = tableName + JOB_NAME_SUFFIX;
        String jobStatus = getLastExecutionStatus(jobName);
        return JobStatus.builder().table(tableName).jobStatus(jobStatus).build();
    }

    private String getLastExecutionStatus (String jobName){
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
    
    public Set<String> triggerAllMigrationJobs() {
        Set<String> alreadyTriggeredTables = new HashSet<>();
        for(String tableName:dbConfigReader.getSourceTableNames()){
            try{
                triggerOneMigrationJob(tableName);
            }catch(JobAlreadyRuningException e){
                alreadyTriggeredTables.add(tableName);
            }
        }
        return alreadyTriggeredTables;
    }

}
