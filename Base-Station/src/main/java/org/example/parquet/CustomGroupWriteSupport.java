package org.example.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

import java.util.Map;
import java.util.Objects;

import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;

public class CustomGroupWriteSupport extends WriteSupport<Group> {
    public static final String PARQUET_EXAMPLE_SCHEMA = "parquet.example.schema";
    private final Map<String, String> extraMetaData;
    private MessageType schema;
    private GroupWriter groupWriter;

    public CustomGroupWriteSupport(MessageType type, Map<String, String> extraMetaData) {
        this.schema = schema;
        this.extraMetaData = extraMetaData;
    }

    public static MessageType getSchema(Configuration configuration) {
        return parseMessageType(Objects.requireNonNull(configuration.get(PARQUET_EXAMPLE_SCHEMA), PARQUET_EXAMPLE_SCHEMA));
    }

    @Override
    public String getName() {
        return "custom";
    }
    @Override
    public WriteContext init(Configuration configuration) {
        if (schema == null) {
            schema = getSchema(configuration);
        }
        return new WriteContext(getSchema(configuration), extraMetaData);
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        groupWriter = new GroupWriter(recordConsumer, schema);
    }

    @Override
    public void write(Group group) {
        groupWriter.write(group);
    }
}
