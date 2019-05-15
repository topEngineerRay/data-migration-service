package com.sap.ngom.datamigration.model.verification;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class MD5Result {

    private VerifiedStatus verifiedStatus;
    private String md5Value;

    public enum VerifiedStatus{
        CONSISTENT,INCONSISTENT,CHECKAGAIN
    }
}
