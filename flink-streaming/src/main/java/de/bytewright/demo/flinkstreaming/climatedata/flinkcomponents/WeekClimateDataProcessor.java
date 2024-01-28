package de.bytewright.demo.flinkstreaming.climatedata.flinkcomponents;

import de.bytewright.demo.flinkstreaming.climatedata.dto.ClimateRecord;
import de.bytewright.demo.flinkstreaming.climatedata.dto.WeekClimateRecord;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;

import java.time.ZonedDateTime;

public class WeekClimateDataProcessor extends ProcessAllWindowFunction<ClimateRecord, WeekClimateRecord, TimeWindow> {
    private static final Logger LOGGER = org.slf4j.LoggerFactory.getLogger(WeekClimateDataProcessor.class);

    @Override
    public void process(ProcessAllWindowFunction<ClimateRecord, WeekClimateRecord, TimeWindow>.Context context, Iterable<ClimateRecord> elements, Collector<WeekClimateRecord> out) throws Exception {
        ZonedDateTime min = null;
        ZonedDateTime max = null;
        double sumTemp = 0d;
        int count = 0;
        for (ClimateRecord element : elements) {
            if (min == null || element.time().isBefore(min)) min = element.time();
            if (max == null || element.time().isAfter(max)) max = element.time();
            sumTemp += element.climateData().getTagesmittelTemperatur();
            count++;
        }
        WeekClimateRecord record = new WeekClimateRecord(min, max, sumTemp, count);
        out.collect(record);
    }
}
