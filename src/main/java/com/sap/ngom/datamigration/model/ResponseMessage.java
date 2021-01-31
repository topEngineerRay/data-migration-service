package com.sap.ngom.datamigration.model;



import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder


@JsonInclude(Include.NON_NULL)
public class ResponseMessage {

    private Status status;
    private String message;
    private Object detail;

}
