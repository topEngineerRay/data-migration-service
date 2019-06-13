package com.sap.ngom.datamigration.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.listener.ChunkListenerSupport;
import org.springframework.batch.core.scope.context.ChunkContext;

import java.time.LocalDateTime;

public class ChunkExecutionListener extends ChunkListenerSupport {

    private static final Logger logger = LoggerFactory.getLogger(ChunkExecutionListener.class);

    @Override
    public void afterChunk(ChunkContext context) {
        logger.info("A chunk commit successfully at:" + LocalDateTime.now());
        super.afterChunk(context);
    }

}