package org.example.weatherstation;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class Weather {
    @JsonProperty("humidity")
    private int humidity;
    @JsonProperty("temperature")
    private int temperature;
    @JsonProperty("wind_speed")
    private int windSpeed;
}
