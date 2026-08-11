package org.example.job_builder.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.example.event.PaymentEvent;

import java.io.IOException;

public class PaymentEventDeserializationSchema implements DeserializationSchema<PaymentEvent> {

    private static final ObjectMapper MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());

    @Override
    public PaymentEvent deserialize(byte[] message) throws IOException {
        return MAPPER.readValue(message, PaymentEvent.class);
    }

    @Override
    public boolean isEndOfStream(PaymentEvent nextElement) {
        return false;
    }

    @Override
    public TypeInformation<PaymentEvent> getProducedType() {
        return TypeInformation.of(PaymentEvent.class);
    }

}
