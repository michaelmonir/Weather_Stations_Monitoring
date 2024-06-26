package com.example.bitcask.Message;

import lombok.Data;

@Data
public class Message {

    public static final int MESSAGE_SIZE = 38;

    private Long station_id;
    private Long s_no;
    private short battery_status;
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

    public Message(long key) {
        this.station_id = key;
        this.s_no = key;
        this.battery_status = (short)key;
        this.status_timestamp = key;
        this.humidity = (int)key;
        this.temperature = (int)key;
        this.wind_speed = (int)key;
    }

    @Override
    public String toString() {
        return "Message{" +
                "station_id=" + station_id +
                ", s_no=" + s_no +
                ", battery_status=" + battery_status +
                ", status_timestamp=" + status_timestamp +
                ", humidity=" + humidity +
                ", temperature=" + temperature +
                ", wind_speed=" + wind_speed +
                '}';
    }
}
