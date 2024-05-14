package org.example.parquet.writers;

import org.example.models.WeatherMessage;

public interface Archiver {
    void receiveWeatherMessage(WeatherMessage weatherMessage);
    void close() throws Exception;
}
