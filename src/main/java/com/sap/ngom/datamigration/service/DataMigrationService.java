package com.sap.ngom.datamigration.service;

import com.sap.ngom.datamigration.configuration.MyItemWriter;
import com.sap.ngom.datamigration.exception.BadRequestValidationException;
import com.sap.ngom.datamigration.exception.BatchJobException;
import com.sap.ngom.datamigration.exception.SourceTableNotDefinedException;
import com.sap.ngom.datamigration.listener.BPStepListener;
import com.sap.ngom.datamigration.listener.JobCompletionNotificationListener;
import com.sap.ngom.datamigration.processor.CustomItemProcessor;
import com.sap.ngom.datamigration.util.DBConfigReader;
import com.sap.ngom.datamigration.util.TenantHelper;
import org.apache.catalina.connector.Response;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.SimpleJob;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.ColumnMapRowMapper;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

@Service
public class DataMigrationService {

    private static final int SKIP_LIMIT = 10;

    @Autowired
    private JobLauncher jobLauncher;

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


    public ResponseEntity triggerOneMigrationJob(String tableName) {
        //we have two different type of jobs: FlowJob and SimpleJob
        tableNameValidation(tableName);
        String jobName = tableName + "_" + "MigrationJobDynamic";
        String jobParameter = jobName;
        JobParameters jobParameters = getJobParameters(jobParameter, jobName);
            List<Step> stepList = new ArrayList<Step>();
            List<String> tenants = tenantHelper.getAllTenants(tableName);
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
                runJob(migrationJob, jobParameters);
            }
        return ResponseEntity.ok().build();
    }

    public void runJob(SimpleJob job, JobParameters jobParameters){
        Runnable runnable = () -> {
            try {
                JobExecution jobExecution =  jobLauncher.run(job, jobParameters);
            } catch (JobExecutionAlreadyRunningException e) {
                e.printStackTrace();
            } catch (JobRestartException e) {
                e.printStackTrace();
            } catch (JobInstanceAlreadyCompleteException e) {
                e.printStackTrace();
            } catch (JobParametersInvalidException e) {
                e.printStackTrace();
            }
        };
        Thread threadRunJob = new Thread(runnable);
        threadRunJob.start();
    }

    private JobParameters getJobParameters(String jobParameter, String jobName) {
        JobParametersBuilder jobBuilder= new JobParametersBuilder();
        jobBuilder.addString(jobName,jobParameter);
        return jobBuilder.toJobParameters();
    }

    private void tableNameValidation(String tableName) {
        if(!dbConfigReader.getSourceTableNames().contains(tableName)){
            throw new SourceTableNotDefinedException("There is no table:"+tableName+"in the database");
        }
    }

    private Step createOneStep(String tenant, String table) {
        //not skip any exceptions now

        String targetNameSpace = dbConfigReader.getTargetNameSpace();

        Step tenantSpecificStep = stepBuilderFactory.get(table + "_" + tenant + "_" + "MigrationStepDynamic")
                .listener(new BPStepListener(tenant))
                .<Map<String, Object>, Map<String, Object>>chunk(10).faultTolerant().noSkip(Exception.class).skipLimit(SKIP_LIMIT)
                .reader(itemReader(dataSource, table, tenant))
                .processor(new CustomItemProcessor())
                .writer(myItemwriter(detinationDataSource, table, targetNameSpace)).faultTolerant().noSkip(Exception.class).skipLimit(SKIP_LIMIT)
                .build();

        return tenantSpecificStep;
    }

    public JdbcCursorItemReader<Map<String, Object>> itemReader(final DataSource dataSource,String tableName, String tenant) {
        //get bp table data from postgresql
        JdbcCursorItemReader<Map<String, Object>> itemReader = new JdbcCursorItemReader<>();
        itemReader.setDataSource(dataSource);
        //need to check if all service have the same name convention
        itemReader.setSql("select * from "+tableName+ " where tenant_id ='" + tenant + "'");
        itemReader.setRowMapper(new ColumnMapRowMapper());
        return itemReader;
    }

    public ItemWriter<Map<String, Object>> myItemwriter(final DataSource dataSource, final String tableName, final String targetNameSpace) {
        // insert into hana
        MyItemWriter myItemWriter = new MyItemWriter(dataSource, tableName, targetNameSpace);
        return myItemWriter;
    }

    public void migrationFailedRecordRetry(String tableName, String pkid) {

    }

    private static JobParameters generateJobParams() {
        return new JobParametersBuilder().addDate("date", new Date()).toJobParameters();
    }

}
