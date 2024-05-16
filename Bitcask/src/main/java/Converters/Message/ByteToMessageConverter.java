package Converters.Message;

import Converters.ByteToDatatypeConverters;
import com.example.bitcask.Message.Message;

public class ByteToMessageConverter {

    ByteToDatatypeConverters reader;

    public ByteToMessageConverter(byte[] bytes) {
        this.reader = new ByteToDatatypeConverters(bytes);
    }

    public Message convert() {
        Long station_id = reader.readLong();
        Long s_no = reader.readLong();
        short battery_status = reader.readShort();
        Long status_timestamp = reader.readLong();
        int humidity = reader.readInt();
        int temperature = reader.readInt();
        int wind_speed = reader.readInt();
        return new Message(station_id, s_no, battery_status, status_timestamp, humidity, temperature, wind_speed);
    }
}
