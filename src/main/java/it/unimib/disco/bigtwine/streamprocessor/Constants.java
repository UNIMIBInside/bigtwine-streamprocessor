package it.unimib.disco.bigtwine.streamprocessor;

public class Constants {
    public static final String KAFKA_BOOTSTRAP_SERVERS = "kafka:9092";

    public static final String NER_INPUT_TOPIC = "ner-requests";
    public static final String NER_OUTPUT_TOPIC = "ner-responses.%s";

    public static final String NEL_INPUT_TOPIC = "nel-requests";
    public static final String NEL_OUTPUT_TOPIC = "nel-responses.%s";

    public static final String LINKRESOLVER_INPUT_TOPIC = "linkresolver-requests";
    public static final String LINKRESOLVER_OUTPUT_TOPIC = "linkresolver-responses.%s";

    public static final String GEODECODER_INPUT_TOPIC = "geodecoder-requests";
    public static final String GEODECODER_OUTPUT_TOPIC = "geodecoder-responses.%s";
}
