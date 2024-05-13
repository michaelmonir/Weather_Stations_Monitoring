package org.example.weatherstatusmsg;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;


@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class WeatherStatusMsg implements Serializable {
    private int stationId;
    private int sNo;
    private BatteryStatus batteryStatus;
    private Long statusTimestamp;
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