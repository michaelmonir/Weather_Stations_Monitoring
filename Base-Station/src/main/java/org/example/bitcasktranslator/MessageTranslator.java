package org.example.bitcasktranslator;

import org.example.models.WeatherMessage;

public class MessageTranslator {
    public byte [] translate(WeatherMessage message) {
        byte [] bytes = new byte[38];
        int index = 0;
        long station_id = message.getStationId();
        for (int i = 0; i < 8; i++) {
            bytes[index++] = (byte) (station_id >> 56);
            station_id <<= 8;
        }
        long s_no = message.getSNo();
        for (int i = 0; i < 8; i++) {
            bytes[index++] = (byte) (s_no >> 56);
            s_no <<= 8;
        }
        String battery_status = message.getBatteryStatus();
        index++;
        switch (battery_status.toLowerCase()) {
            case "low":
                bytes[index++] = (byte) 0;
                break;
            case "medium":
                bytes[index++] = (byte) 1;
                break;
            case "high":
                bytes[index++] = (byte) 2;
                break;
        }
        long status_timestamp = message.getStatusTimestamp();
        for (int i = 0; i < 8; i++) {
            bytes[index++] = (byte) (status_timestamp >> 56);
            status_timestamp <<= 8;
        }
        int humidity = message.getWeather().getHumidity();
        for (int i = 0; i < 4; i++) {
            bytes[index++] = (byte) (humidity >> 24);
            humidity <<= 8;
        }
        int temperature = message.getWeather().getTemperature();
        for (int i = 0; i < 4; i++) {
            bytes[index++] = (byte) (temperature >> 24);
            temperature <<= 8;
        }
        int wind_speed = message.getWeather().getWindSpeed();
        for (int i = 0; i < 4; i++) {
            bytes[index++] = (byte) (wind_speed >> 24);
            wind_speed <<= 8;
        }
        return bytes;
    }
}
