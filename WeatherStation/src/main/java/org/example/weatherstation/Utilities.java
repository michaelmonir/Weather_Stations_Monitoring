package org.example.weatherstation;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class Utilities {
    private final Gson gson = new Gson();
    private int stationId;
    private final Random random = new Random();
    private final AtomicInteger sequenceNumber = new AtomicInteger(0);
    public String changeMsgToJson(WeatherStatusMsg weatherStatusMsg) {
        return new ObjectMapper().valueToTree(weatherStatusMsg).toString();
    }

    public Utilities(int stationId) {
        this.stationId = stationId;
    }
    public BatteryStatus generateBatteryStatus() {
        double rand = Math.random();
        if (rand < 0.3) return BatteryStatus.low;
        if (rand < 0.7) return BatteryStatus.medium;
        return BatteryStatus.high;
    }
    public int getRandomStationId() {
        return this.stationId;
    }

    public int getNextSequenceNumber() {
        return sequenceNumber.incrementAndGet();
    }

    public int getRandomHumidity() {
        return random.nextInt(101);
    }

    public int getRandomTemperature() {
        return random.nextInt(121);
    }

    public int getRandomWindSpeed() {
        return random.nextInt(201);
    }

    public double getRandomNoBetweenXAndNegativeX(int x){
        return -x + Math.random() * (2 * x);
    }
}
