package com.sap.ngom.datamigration.listener;

import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.stereotype.Component;

@Component
public class BPChunkListener implements ChunkListener {
    @Override public void beforeChunk(ChunkContext context) {

    }

    @Override public void afterChunk(ChunkContext context) {
        System.out.println(context.getStepContext().getStepExecutionContext());
    }

    @Override public void afterChunkError(ChunkContext context) {

    }
}
