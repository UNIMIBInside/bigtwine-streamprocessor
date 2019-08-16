package it.unimib.disco.bigtwine.streamprocessor.request;

import com.google.common.collect.Iterables;
import it.unimib.disco.bigtwine.commons.messaging.GeoDecoderRequestMessage;
import it.unimib.disco.bigtwine.commons.models.dto.LocationDTO;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeoDecoderRequestMessageBuilder extends AbstractRequestMessageBuilder<GeoDecoderRequestMessage, LocationDTO>
        implements AllWindowFunction<LocationDTO, GeoDecoderRequestMessage, TimeWindow> {
    private static final Logger LOG = LoggerFactory.getLogger(GeoDecoderRequestMessageBuilder.class);

    private String decoder;

    public GeoDecoderRequestMessageBuilder() {
        super();
    }

    public GeoDecoderRequestMessageBuilder(String outputTopic, String requestIdPrefix, String decoder) {
        super(outputTopic, requestIdPrefix);
        this.decoder = decoder;
    }

    public String getDecoder() {
        return decoder;
    }

    public void setDecoder(String decoder) {
        this.decoder = decoder;
    }

    @Override
    protected GeoDecoderRequestMessage buildRequest(Iterable<LocationDTO> items) {
        GeoDecoderRequestMessage request = new GeoDecoderRequestMessage();
        this.setCommons(request);
        request.setDecoder(decoder);
        request.setLocations(Iterables.toArray(items, LocationDTO.class));

        return request;
    }

    @Override
    public void apply(TimeWindow window, Iterable<LocationDTO> tweets, Collector<GeoDecoderRequestMessage> out) throws Exception {
        if (!tweets.iterator().hasNext()) {
            return;
        }

        GeoDecoderRequestMessage request = this.buildRequest(tweets);
        out.collect(request);

        LOG.debug("Starting geo decoder processing {} location", request.getLocations().length);
    }
}