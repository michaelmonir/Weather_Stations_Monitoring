package org.example.WeatherStation;


import com.google.gson.Gson;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class Utilities {
    private final Gson gson = new Gson();
    private final Random random = new Random();
    private final AtomicInteger sequenceNumber = new AtomicInteger(0);
    public String changeMsgToJson(WeatherStatusMsg weatherStatusMsg) {
        return gson.toJson(weatherStatusMsg);
    }
    public BatteryStatus generateBatteryStatus() {
        double rand = Math.random();
        if (rand < 0.3) return BatteryStatus.LOW;
        if (rand < 0.7) return BatteryStatus.MEDIUM;
        return BatteryStatus.HIGH;
    }
    public int getRandomStationId() {
        return random.nextInt(10) + 1;
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
