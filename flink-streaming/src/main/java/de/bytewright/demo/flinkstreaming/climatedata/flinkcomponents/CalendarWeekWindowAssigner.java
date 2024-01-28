package de.bytewright.demo.flinkstreaming.climatedata.flinkcomponents;

import de.bytewright.demo.flinkstreaming.climatedata.dto.ClimateRecord;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.slf4j.Logger;

import java.time.ZonedDateTime;
import java.time.temporal.WeekFields;
import java.util.Collection;
import java.util.Collections;

// create a custom window assigner that assigns elements to windows by calendar week
public class CalendarWeekWindowAssigner extends WindowAssigner<Object, TimeWindow> {
    private static final Logger LOGGER = org.slf4j.LoggerFactory.getLogger(CalendarWeekWindowAssigner.class);

    @Override
    public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {

        // get the calendar week of the element
        ClimateRecord climateRecord = (ClimateRecord) element;
        ZonedDateTime zonedDateTime = climateRecord.time();

        // get the start and end of the week
        long start = zonedDateTime.with(WeekFields.ISO.dayOfWeek(), 1).toInstant().toEpochMilli();
        long end = zonedDateTime.with(WeekFields.ISO.dayOfWeek(), 7).toInstant().toEpochMilli();

        // create a time window for the week
        return Collections.singletonList(new TimeWindow(start, end));
    }

    @Override
    public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {

        return EventTimeTrigger.create();
    }


    @Override
    public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
        // use the default time window serializer
        return new TimeWindow.Serializer();
    }

    @Override
    public boolean isEventTime() {
        // use event-time semantics
        return true;
    }
}

