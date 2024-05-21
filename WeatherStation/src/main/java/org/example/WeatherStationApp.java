package org.example;
import org.example.weatherstation.OpenMeteoWeatherStation;
import org.example.weatherstation.WeatherStatusMsg;
import org.example.weatherstation.Utilities;

import org.example.kafka.Producer;
public class WeatherStationApp {
    public static void main(String[] args) throws Exception {
        int flag = Integer.parseInt(args[0]);
        System.out.println("Flag: " + flag);
        if (flag == 1) {
            OpenMeteoWeatherStation openMeteoWeather = new OpenMeteoWeatherStation();
            while (true) {
                openMeteoWeather.send();
            }
        } else {
            WeatherStatusMsg weatherStatusMsg = new WeatherStatusMsg();
            Producer producer = new Producer();
            Utilities utilities = new Utilities();
            while (true) {
                String temp = weatherStatusMsg.generateWeatherStatusMsg(utilities);
                if (temp != null)producer.send(temp);
                Thread.sleep(1000);
            }
        }
    }
}