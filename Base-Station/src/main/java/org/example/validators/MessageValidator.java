package org.example.validators;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;

import java.io.InputStream;
import java.util.Set;

public class MessageValidator {
    private final JsonSchema schema;
    private final ObjectMapper objectMapper;

    public MessageValidator(InputStream schemaInputStream) {
        JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7);
        this.schema = factory.getSchema(schemaInputStream);
        this.objectMapper = new ObjectMapper();
    }

    public Set<ValidationMessage> validateAndProcessMessage(String message) {
        try {
            JsonNode jsonNode = objectMapper.readTree(message);
            return schema.validate(jsonNode);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private void processValidMessage(JsonNode jsonNode) {
        long stationId = jsonNode.get("station_id").asLong();
        long sequenceNumber = jsonNode.get("s_no").asLong();
        String batteryStatus = jsonNode.get("battery_status").asText();
        long statusTimestamp = jsonNode.get("status_timestamp").asLong();

        JsonNode weatherNode = jsonNode.get("weather");
        int humidity = weatherNode.get("humidity").asInt();
        int temperature = weatherNode.get("temperature").asInt();
        int windSpeed = weatherNode.get("wind_speed").asInt();

        // Do something with the message
        System.out.println("Station ID: " + stationId);
        System.out.println("Sequence Number: " + sequenceNumber);
        System.out.println("Battery Status: " + batteryStatus);
        System.out.println("Status Timestamp: " + statusTimestamp);
        System.out.println("Humidity: " + humidity);
        System.out.println("Temperature: " + temperature);
        System.out.println("Wind Speed: " + windSpeed);

    }
}
