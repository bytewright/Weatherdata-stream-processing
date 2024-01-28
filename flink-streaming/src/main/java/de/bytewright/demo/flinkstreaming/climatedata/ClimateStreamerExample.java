package de.bytewright.demo.flinkstreaming.climatedata;

import de.bytewright.demo.flinkstreaming.climatedata.dto.ClimateData;
import de.bytewright.demo.flinkstreaming.climatedata.dto.ClimateRecord;
import de.bytewright.demo.flinkstreaming.climatedata.dto.WeekClimateRecord;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

@Component
public class ClimateStreamerExample implements InitializingBean, DisposableBean {
    private static final Logger LOGGER = org.slf4j.LoggerFactory.getLogger(ClimateStreamerExample.class);
    private StreamExecutionEnvironment env;

    public void start() {
        ClimateDataProcessingJob job = new ClimateDataProcessingJob();
        job.start(env);
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void destroy() throws Exception {
        env.close();
    }

    @Override
    public void afterPropertiesSet() {
        env = LocalStreamEnvironment.createLocalEnvironment();
        env.getConfig().registerKryoType(ClimateData.class);
        env.getConfig().registerKryoType(ClimateRecord.class);
        env.getConfig().registerKryoType(WeekClimateRecord.class);
        LOGGER.info("LocalStreamEnvironment created: {}", env);
    }
}
