package de.bytewright.demo.flinkstreaming.climatedata.dto;

import java.time.ZonedDateTime;
import java.util.Objects;

public final class ClimateRecord {
    private final ZonedDateTime time;
    private final ClimateData climateData;

    public ClimateRecord(ZonedDateTime time, ClimateData climateData) {
        this.time = time;
        this.climateData = climateData;
    }

    public ZonedDateTime time() {
        return time;
    }

    public ClimateData climateData() {
        return climateData;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (ClimateRecord) obj;
        return Objects.equals(this.time, that.time) &&
                Objects.equals(this.climateData, that.climateData);
    }

    @Override
    public int hashCode() {
        return Objects.hash(time, climateData);
    }

    @Override
    public String toString() {
        return "ClimateRecord[" +
                "time=" + time + ", " +
                "climateData=" + climateData + ']';
    }

}
