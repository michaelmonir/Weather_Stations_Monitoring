package org.example;

import org.example.WeatherStation.OpenMeteoWeatherStation;
import org.example.WeatherStation.Utilities;
import org.example.WeatherStation.WeatherStatusMsg;
import org.example.kafka.Producer;


public class Main {
    public static void main(String[] args) throws Exception {

//        if (false) {
//            OpenMeteoWeatherStation openMeteoWeather = new OpenMeteoWeatherStation();
//            while (true) {
//                openMeteoWeather.send();
//            }
//        } else {
//            WeatherStatusMsg weatherStatusMsg = new WeatherStatusMsg();
//            Producer producer = new Producer();
//            Utilities utilities = new Utilities();
//            while (true) {
//                String temp = weatherStatusMsg.generateWeatherStatusMsg(utilities);
//                if (temp != null)producer.send(temp);
//                Thread.sleep(1000);
//            }
//        }
    }
}