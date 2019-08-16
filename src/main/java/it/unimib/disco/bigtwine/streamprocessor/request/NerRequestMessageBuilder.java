package it.unimib.disco.bigtwine.streamprocessor.request;

import com.google.common.collect.Iterables;
import it.unimib.disco.bigtwine.commons.messaging.NerRequestMessage;
import it.unimib.disco.bigtwine.commons.models.dto.BasicTweetDTO;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class NerRequestMessageBuilder extends AbstractRequestMessageBuilder<NerRequestMessage, BasicTweetDTO>
        implements AllWindowFunction<BasicTweetDTO, NerRequestMessage, TimeWindow> {
    private static final Logger LOG = LoggerFactory.getLogger(NerRequestMessageBuilder.class);

    private String recognizer;

    public NerRequestMessageBuilder() {
        super();
    }

    public NerRequestMessageBuilder(String outputTopic, String requestIdPrefix, String recognizer) {
        super(outputTopic, requestIdPrefix);
        this.recognizer = recognizer;
    }

    public String getRecognizer() {
        return recognizer;
    }

    public void setRecognizer(String recognizer) {
        this.recognizer = recognizer;
    }

    @Override
    protected NerRequestMessage buildRequest(Iterable<BasicTweetDTO> tweets) {
        NerRequestMessage request = new NerRequestMessage();
        this.setCommons(request);
        request.setRecognizer(this.recognizer);
        request.setTweets(Iterables.toArray(tweets, BasicTweetDTO.class));

        return request;
    }

    @Override
    public void apply(TimeWindow window, Iterable<BasicTweetDTO> tweets, Collector<NerRequestMessage> out) throws Exception {
        if (!tweets.iterator().hasNext()) {
            return;
        }

        NerRequestMessage request = this.buildRequest(tweets);
        out.collect(request);

        LOG.debug("Starting ner processing {} tweets", request.getTweets().length);
    }
}