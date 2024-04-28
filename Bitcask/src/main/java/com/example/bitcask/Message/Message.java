package com.example.bitcask.Message;

import lombok.Data;

@Data
public class Message {

    public static final int MESSAGE_SIZE = 38;

    private Long station_id;
    private Long s_no;
    private short battery_status; // low: 0, medium: 1, high: 2
    private Long status_timestamp;
    private int humidity;
    private int temperature;
    private int wind_speed;

    public Message(Long station_id, Long s_no, short battery_status, Long status_timestamp, int humidity, int temperature, int wind_speed) {
        this.station_id = station_id;
        this.s_no = s_no;
        this.battery_status = battery_status;
        this.status_timestamp = status_timestamp;
        this.humidity = humidity;
        this.temperature = temperature;
        this.wind_speed = wind_speed;
    }
}
