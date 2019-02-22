# data-migration-service

This data migration service is designed as a reuse service to do data migration from postgresql to HANA as part of Move2HANA.


# How to Run

## Run it Locally

### Prerequisites

Make sure the following dependencies of the service (e.g. business-partner) your want to do data migration are ready:
* postgresql db installed on local host
* hdi-deployer app is deployed on SCP
* managed-hana backing service is created on SCP

Prepare some data in postgresql database.

### Run Application

1. Add VCAP_SERVICES environment variable to specify the managed-hana information.
2. Specify spring profile.
3. Go to the main class DataMigration run as java application.
4. Test data migration by call endpoints provided.

## Run it on SCP



# Endpoints
In this service, there are some endpoints provided to conduct a data migration, including triggering data migration, data verification etc.

|Category|Endpoint|Description|
|:-------------|:-------------|:-------------|
| Trigger | POST /jobs | Trigger data migration action for all the configured tables. |
| Trigger | POST /jobs/{tableName} | Trigger data migration action for one specified table. |
| Trigger | POST /jobs/migrateSingleRecord | Trigger data migration action for one record. |
| Monitoring | GET /jobs | Check data migration job status for all the configured tables. |
| Monitoring | GET /jobs/{tableName} | Check data migration job status for one specified table. |
| Cleanup | POST /data/cleanup | Trigger clean up data in target database for all the configured tables. |
| Cleanup | POST /data/cleanup/{tableName} | Trigger clean up data in target db for specifed table. |
| Verification | POST /data/verification | Trigger data verification for all the configured tables between source db and target db. |
| Verification | POST /data/verification/{tableName} | Trigger data verification for one specifed table between source db and target db. |

For detail usage, please check wiki.

# References
* Spring Batch: https://spring.io/projects/spring-batch
