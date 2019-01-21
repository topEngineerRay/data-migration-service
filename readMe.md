# Description
The target of this service is to provide apis to read data from postgresql and insert into hanaDB

# How to run this current POC on local

Step1:
Add VCAP_SERVICES to specify the managed-hana information as the run arguments

Step2:
Set the VM -options to: -Dspring.profiles.active=postgres

Then go to the application-postgresql.properties, update below postgresql properties as yours:
spring.datasource.jdbc-url=jdbc:postgresql://localhost:5432/dbname
spring.datasource.username= username
spring.datasource.password= pwd

Step3:
change the code in the Class BatchConfiguration.java, update the method:JdbcPagingItemReader
update the tenant in this line: provider.setWhereClause("where tenant_id='ray4'"); to provider.setWhereClause("where tenant_id='yourtenant'");

Step4:
As we already have the hdideployer in kfp test space, just run DataMigration.java as how to run other spring boot application

Step5:
trigger migration job by api, in this poc, we can trigger migration for bp table with the below url in postman
{{url}}/v1/bpMigration/migrationOneJob/bpMigrationJob

Step6:
Go to hana db to manually verify the results.