package it.unimib.disco.bigtwine.streamprocessor.request;

import com.google.common.collect.Iterables;
import it.unimib.disco.bigtwine.commons.messaging.NelRequestMessage;
import it.unimib.disco.bigtwine.commons.models.dto.RecognizedTweetDTO;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NelRequestMessageBuilder extends AbstractRequestMessageBuilder<NelRequestMessage, RecognizedTweetDTO>
        implements AllWindowFunction<RecognizedTweetDTO, NelRequestMessage, TimeWindow> {
    private static final Logger LOG = LoggerFactory.getLogger(NelRequestMessageBuilder.class);
    private String linker;

    public NelRequestMessageBuilder() {
        super();
    }

    public NelRequestMessageBuilder(String outputTopic, String requestIdPrefix, String linker) {
        super(outputTopic, requestIdPrefix);
        this.linker = linker;
    }

    public String getLinker() {
        return linker;
    }

    public void setLinker(String linker) {
        this.linker = linker;
    }

    @Override
    protected NelRequestMessage buildRequest(Iterable<RecognizedTweetDTO> items) {
        NelRequestMessage request = new NelRequestMessage();
        this.setCommons(request);
        request.setLinker(linker);
        request.setTweets(Iterables.toArray(items, RecognizedTweetDTO.class));

        return request;
    }

    @Override
    public void apply(TimeWindow window, Iterable<RecognizedTweetDTO> tweets, Collector<NelRequestMessage> out) throws Exception {
        if (!tweets.iterator().hasNext()) {
            return;
        }

        NelRequestMessage request = this.buildRequest(tweets);
        LOG.debug("Starting nel processing {} tweets", request.getTweets().length);

        out.collect(request);
    }
}
