package com.sap.ngom.datamigration.model.verification;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SourceTableInfo {
    private int postgresRecordsCount;
    private String postgresMd5Sql;
}
