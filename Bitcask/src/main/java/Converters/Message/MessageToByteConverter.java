package Converters.Message;

import Converters.DatatypeToByteConverters;
import com.example.bitcask.Message.Message;

public class MessageToByteConverter {

    private Message message;
    private DatatypeToByteConverters writer;

    public MessageToByteConverter(Message message) {
        this.message = message;
        this.writer = new DatatypeToByteConverters(Message.MESSAGE_SIZE);
    }

    public byte[] convert() {
        writer.writeLong(message.getStation_id());
        writer.writeLong(message.getS_no());
        writer.writeShort(message.getBattery_status());
        writer.writeLong(message.getStatus_timestamp());
        writer.writeInt(message.getHumidity());
        writer.writeInt(message.getTemperature());
        writer.writeInt(message.getWind_speed());

        return this.writer.getBytes();
    }
}
