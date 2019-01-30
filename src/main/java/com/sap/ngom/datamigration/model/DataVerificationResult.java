package com.sap.ngom.datamigration.model;


import lombok.*;

import java.util.List;
import java.util.Map;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DataVerificationResult {

    private Status status;
    private String message;
    private Detail details;

}
