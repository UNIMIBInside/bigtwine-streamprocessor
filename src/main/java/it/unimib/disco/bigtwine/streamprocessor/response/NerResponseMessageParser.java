package it.unimib.disco.bigtwine.streamprocessor.response;

import it.unimib.disco.bigtwine.commons.messaging.NerResponseMessage;
import it.unimib.disco.bigtwine.commons.models.dto.RecognizedTweetDTO;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

public class NerResponseMessageParser extends AbstractResponseMessageParser<NerResponseMessage, RecognizedTweetDTO>
    implements FlatMapFunction<String, RecognizedTweetDTO> {
    private static final Logger LOG = LoggerFactory.getLogger(NerResponseMessageParser.class);
    private transient JsonDeserializer<NerResponseMessage> deserializer;

    public NerResponseMessageParser() {
    }

    public NerResponseMessageParser(String outputTopic) {
        super(outputTopic);
    }

    @Override
    public JsonDeserializer<NerResponseMessage> getDeserializer() {
        if (deserializer == null) {
            deserializer = new JsonDeserializer<>(NerResponseMessage.class);
        }
        return deserializer;
    }

    @Override
    public void flatMap(String value, Collector<RecognizedTweetDTO> out) throws Exception {
        NerResponseMessage res = this.parse(value);

        for (RecognizedTweetDTO tweet : res.getTweets())  {
            out.collect(tweet);
        }

        LOG.debug("Finished ner processing, {} tweets collected", res.getTweets().length);
    }
}
