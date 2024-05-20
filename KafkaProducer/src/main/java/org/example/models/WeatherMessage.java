package org.example.models;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import java.io.Serializable;
import lombok.Data;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Data
@AllArgsConstructor
@NoArgsConstructor
public class WeatherMessage implements Serializable {
    @JsonProperty("station_id")
    private int stationId;

    @JsonProperty("s_no")
    private int sNo;

    @JsonProperty("battery_status")
    private String batteryStatus;

    @JsonProperty("status_timestamp")
    private int statusTimestamp;

    private WeatherData weather;
    public static WeatherMessage fromJson(String message) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.readValue(message, WeatherMessage.class);
        } catch (Exception e) {
            System.out.println("Error parsing message: " + message);
            return null;
        }
    }

    public static class WeatherData {
        @JsonProperty("humidity")
        private int humidity;
        @JsonProperty("temperature")
        private int temperature;
        @JsonProperty("wind_speed")
        private int windSpeed;
    }


}


//    public Group toParquetGroup(MessageType schema) {
//        GroupFactory groupFactory = new SimpleGroupFactory(schema);
//        Group weatherStatusGroup = groupFactory.newGroup();
//        weatherStatusGroup.add("station_id", Long.parseLong(stationId));
//        weatherStatusGroup.add("s_no", Long.parseLong(sNo));
//        weatherStatusGroup.add("battery_status", batteryStatus);
//        weatherStatusGroup.add("status_timestamp", Long.parseLong(statusTimestamp));
//        weatherStatusGroup.add("humidity", (int) Float.parseFloat(humidity));
//        weatherStatusGroup.add("temperature", (int) Float.parseFloat(temperature));
//        weatherStatusGroup.add("wind_speed", (int) Float.parseFloat(windSpeed));
//        return weatherStatusGroup;
//    }
//}
