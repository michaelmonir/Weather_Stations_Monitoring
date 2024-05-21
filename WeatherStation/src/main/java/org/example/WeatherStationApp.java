package org.example;
import org.example.weatherstation.OpenMeteoWeatherStation;
import org.example.weatherstation.WeatherStatusMsg;
import org.example.weatherstation.Utilities;

import org.example.kafka.Producer;
public class WeatherStationApp {
    public static void main(String[] args) throws Exception {
        int flag = System.getenv("MOODFLAG") != null ? Integer.parseInt(System.getenv("MOODFLAG")) : 0;
        System.out.println("Flag: " + flag);
        int stationId = System.getenv("STATION_ID") != null ? Integer.parseInt(System.getenv("STATION_ID")) : 0;
        System.out.println("Station ID: " + stationId);
        if (flag == 1) {
            OpenMeteoWeatherStation openMeteoWeather = new OpenMeteoWeatherStation();
            while (true) {
                openMeteoWeather.send();
            }
        } else {
            WeatherStatusMsg weatherStatusMsg = new WeatherStatusMsg();
            Producer producer = new Producer();
            Utilities utilities = new Utilities(stationId );
            while (true) {
                String temp = weatherStatusMsg.generateWeatherStatusMsg(utilities);
                if (temp != null)producer.send(temp);
                Thread.sleep(1000);
            }
        }
    }
}