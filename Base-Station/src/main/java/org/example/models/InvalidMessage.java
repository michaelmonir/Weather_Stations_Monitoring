package org.example.models;

import com.networknt.schema.ValidationMessage;
import lombok.Data;

import java.util.Set;

@Data
public class InvalidMessage {
    private final String message;
    private final Set<ValidationMessage> errors;
}
