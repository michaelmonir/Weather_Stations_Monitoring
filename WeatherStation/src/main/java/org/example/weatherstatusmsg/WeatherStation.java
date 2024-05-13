package org.example.weatherstatusmsg;
import org.example.kafka.Producer;

public class WeatherStation {
    public static void main(String[] args) throws InterruptedException {
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
