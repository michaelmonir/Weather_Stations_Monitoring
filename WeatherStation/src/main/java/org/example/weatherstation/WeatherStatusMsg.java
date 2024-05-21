package org.example.weatherstation;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.json.JSONPropertyName;

import java.io.Serializable;


@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor


public class WeatherStatusMsg implements Serializable {

    @JsonProperty("station_id")
    private int stationId;
    @JsonProperty("s_no")
    private int sNo;
    @JsonProperty("battery_status")
    private BatteryStatus batteryStatus;
    @JsonProperty("status_timestamp")
    private Long statusTimestamp;
    @JsonProperty("weather")
    private Weather weather;

    public String generateWeatherStatusMsg(Utilities utilities) {
        if (Math.random() < 0.1) return null;
        this.batteryStatus = utilities.generateBatteryStatus();
        this.stationId = utilities.getRandomStationId();
        this.sNo = utilities.getNextSequenceNumber();
        this.statusTimestamp = System.currentTimeMillis();
        this.weather = new Weather(utilities.getRandomHumidity(), utilities.getRandomTemperature(), utilities.getRandomWindSpeed());
        return utilities.changeMsgToJson(this);
    }

}