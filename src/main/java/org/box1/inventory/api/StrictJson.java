package org.box1.inventory.api;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.jackson.ObjectMapperCustomizer;
import jakarta.inject.Singleton;

/** Rejects coerced scalars, so "2" and 2.5 are not accepted as an integer quantity. */
@Singleton
public class StrictJson implements ObjectMapperCustomizer {
    @Override
    @SuppressWarnings("deprecation")
    public void customize(ObjectMapper mapper) {
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);
        mapper.configure(DeserializationFeature.ACCEPT_FLOAT_AS_INT, false);
    }
}
