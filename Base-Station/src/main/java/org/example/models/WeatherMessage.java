package org.example.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.schema.MessageType;

import java.io.Serializable;

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
    private Long statusTimestamp;

    @JsonProperty("weather")
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

    public static Object getClassSchema() {
        return new Schema.Parser().parse(
                "{\n" +
                        "  \"type\": \"record\",\n" +
                        "  \"name\": \"WeatherMessage\",\n" +
                        "  \"fields\": [\n" +
                        "    {\"name\": \"station_id\", \"type\": \"long\"},\n" +
                        "    {\"name\": \"s_no\", \"type\": \"long\"},\n" +
                        "    {\"name\": \"battery_status\", \"type\": \"string\"},\n" +
                        "    {\"name\": \"status_timestamp\", \"type\": \"long\"},\n" +
                        "    {\"name\": \"weather\",\n" +
                        "      \"type\": {\n" +
                        "        \"type\": \"record\",\n" +
                        "        \"name\": \"WeatherData\",\n" +
                        "        \"fields\": [\n" +
                        "          {\"name\": \"humidity\", \"type\": \"int\"},\n" +
                        "          {\"name\": \"temperature\", \"type\": \"int\"},\n" +
                        "          {\"name\": \"wind_speed\", \"type\": \"int\"}\n" +
                        "        ]\n" +
                        "      }\n" +
                        "    }\n" +
                        "  ]\n" +
                        "}"
        );
    }

    public Group toParquetGroup(MessageType schema) {
        GroupFactory groupFactory = new SimpleGroupFactory(schema);
        Group weatherStatusGroup = groupFactory.newGroup();
        weatherStatusGroup.add("station_id", Long.parseLong(String.valueOf(stationId)));
        weatherStatusGroup.add("s_no", Long.parseLong(String.valueOf(sNo)));
        weatherStatusGroup.add("battery_status", batteryStatus);
        weatherStatusGroup.add("status_timestamp", Long.parseLong(String.valueOf(statusTimestamp)));
        weatherStatusGroup.add("humidity", (int) Float.parseFloat(String.valueOf(weather.humidity)));
        weatherStatusGroup.add("temperature", (int) Float.parseFloat(String.valueOf(weather.temperature)));
        weatherStatusGroup.add("wind_speed", (int) Float.parseFloat(String.valueOf(weather.windSpeed)));
        return weatherStatusGroup;
    }

    public GenericRecord toAvroRecord(Schema schema) {
        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("station_id", stationId);
        avroRecord.put("s_no", sNo);
        avroRecord.put("battery_status", batteryStatus);
        avroRecord.put("status_timestamp", statusTimestamp);
        avroRecord.put("humidity", weather.humidity);
        avroRecord.put("temperature", weather.temperature);
        avroRecord.put("wind_speed", weather.windSpeed);
        return avroRecord;
    }

    @Data
    public static class WeatherData implements Serializable {
        @JsonProperty("humidity")
        private int humidity;
        @JsonProperty("temperature")
        private int temperature;
        @JsonProperty("wind_speed")
        private int windSpeed;
    }
}
