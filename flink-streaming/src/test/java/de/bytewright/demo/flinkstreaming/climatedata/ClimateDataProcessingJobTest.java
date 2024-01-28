package de.bytewright.demo.flinkstreaming.climatedata;

import de.bytewright.demo.flinkstreaming.climatedata.dto.ClimateData;
import de.bytewright.demo.flinkstreaming.climatedata.dto.ClimateRecord;
import de.bytewright.demo.flinkstreaming.climatedata.dto.WeekClimateRecord;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

class ClimateDataProcessingJobTest {
    @Test
    void name() throws Exception {
        //StreamExecutionEnvironment env = LocalStreamEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = LocalStreamEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.getConfig().registerKryoType(ClimateData.class);
        env.getConfig().registerKryoType(ClimateRecord.class);
        env.getConfig().registerKryoType(WeekClimateRecord.class);
        ClimateDataProcessingJob job = new ClimateDataProcessingJob();
        job.start(env);
        env.execute();
    }
}