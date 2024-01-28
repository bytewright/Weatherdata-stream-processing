package de.bytewright.demo.flinkstreaming.climatedata;

import de.bytewright.demo.flinkstreaming.climatedata.dto.ClimateData;
import de.bytewright.demo.flinkstreaming.climatedata.dto.ClimateRecord;
import de.bytewright.demo.flinkstreaming.climatedata.dto.WeekClimateRecord;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.RestOptions;
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
        env = LocalStreamEnvironment.createLocalEnvironment();
        env.getConfig().registerKryoType(ClimateData.class);
        env.getConfig().registerKryoType(ClimateRecord.class);
        env.getConfig().registerKryoType(WeekClimateRecord.class);
        LOGGER.info("LocalStreamEnvironment created: {}", env);
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
        env = LocalStreamEnvironment.createLocalEnvironmentWithWebUI(getEnvironmentConfig());
        env.getConfig().registerKryoType(ClimateData.class);
        env.getConfig().registerKryoType(ClimateRecord.class);
        env.getConfig().registerKryoType(WeekClimateRecord.class);
        LOGGER.info("LocalStreamEnvironment created: {}", env);
    }

    private Configuration getEnvironmentConfig() {
        Configuration configuration = new Configuration();
        configuration.setString(CoreOptions.TMP_DIRS, "tmp");
        configuration.setInteger(RestOptions.PORT, 8899);
        return configuration;
    }
}
