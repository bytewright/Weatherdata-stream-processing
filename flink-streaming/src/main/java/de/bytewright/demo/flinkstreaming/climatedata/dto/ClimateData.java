package de.bytewright.demo.flinkstreaming.climatedata.dto;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Data
public class ClimateData implements Serializable {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClimateData.class);

  public static final ClimateData EMPTY = new ClimateData();
  int avgOverDays;
  LocalDate recordingDate;
  String qn3;
  String maximaleWindspitze; //fx
  String tagesmittelWindgeschwindigkeit;//fm
  String qn4;
  double niederschlagshoehe;//rsk
  int niederschlagsform;//rskf
  double sonnenscheindauerTagessumme;//sdk
  int schneehoeheTageswert;//shkTag
  double tagesmittelBedeckungsgrad;//nm
  double tagesmittelDampfdruck;//vpm
  double tagesmittelLuftdruck;//pm
  double tagesmittelTemperatur;//tmk
  double tagesmittelRelativeFeuchte;//upm
  double tagesmaximumLufttemperatur;//txk
  double tagesminimumLufttemperatur;//tnk
  double minLufttemperaturBoden;//tkg

  private ClimateData(List<String> strings) throws NumberFormatException, NullPointerException {
    String date = getOrNull(strings, 1);
    recordingDate = date != null ? LocalDate.parse(date, DateTimeFormatter.BASIC_ISO_DATE) : null;
    qn3 = getOrNull(strings, 2);
    maximaleWindspitze = getOrNull(strings, 3);
    tagesmittelWindgeschwindigkeit = getOrNull(strings, 4);
    qn4 = getOrNull(strings, 5);
    niederschlagshoehe = Double.parseDouble(getOrNull(strings, 6));
    niederschlagsform = Integer.parseInt(getOrNull(strings, 7));
    sonnenscheindauerTagessumme = Double.parseDouble(getOrNull(strings, 8));
    schneehoeheTageswert = Integer.parseInt(getOrNull(strings, 9));
    tagesmittelBedeckungsgrad = Double.parseDouble(getOrNull(strings, 10));
    tagesmittelDampfdruck = Double.parseDouble(getOrNull(strings, 11));
    tagesmittelLuftdruck = Double.parseDouble(getOrNull(strings, 12));
    tagesmittelTemperatur = Double.parseDouble(getOrNull(strings, 13));
    tagesmittelRelativeFeuchte = Double.parseDouble(getOrNull(strings, 14));
    tagesmaximumLufttemperatur = Double.parseDouble(getOrNull(strings, 15));
    tagesminimumLufttemperatur = Double.parseDouble(getOrNull(strings, 16));
    minLufttemperaturBoden = Double.parseDouble(getOrNull(strings, 17));
    avgOverDays = 1;
  }

  public ClimateData() {

  }

  private String getOrNull(List<String> strings, int i) {
    return StringUtils.stripToNull(strings.get(i));
  }

  public static ClimateData from(String value) {
    List<String> strings = Arrays.stream(value.split(";"))
        .map(String::trim)
        .collect(Collectors.toList());
    if (strings.size() == 19 && strings.get(0).equals("1270")) {
      try {
        return new ClimateData(strings);
      } catch (NumberFormatException | NullPointerException e) {
        //        throw e;
        //        LOGGER.info("Error while parsing {}", value, e);
      }
    }
    return EMPTY;
  }
}
