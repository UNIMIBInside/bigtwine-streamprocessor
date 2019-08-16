package it.unimib.disco.bigtwine.streamprocessor.request;

import com.google.common.collect.Iterables;
import it.unimib.disco.bigtwine.commons.messaging.LinkResolverRequestMessage;
import it.unimib.disco.bigtwine.commons.models.dto.LinkDTO;
import it.unimib.disco.bigtwine.commons.models.dto.LinkedEntityDTO;
import it.unimib.disco.bigtwine.commons.models.dto.LinkedTweetDTO;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class LinkResolverRequestMessageBuilder extends AbstractRequestMessageBuilder<LinkResolverRequestMessage, LinkDTO>
        implements MapFunction<LinkedTweetDTO, LinkResolverRequestMessage> {
    private static final Logger LOG = LoggerFactory.getLogger(LinkResolverRequestMessageBuilder.class);


    public LinkResolverRequestMessageBuilder() {
    }

    public LinkResolverRequestMessageBuilder(String outputTopic, String requestIdPrefix) {
        super(outputTopic, requestIdPrefix);
    }

    @Override
    protected LinkResolverRequestMessage buildRequest(Iterable<LinkDTO> items) {
        LinkResolverRequestMessage request = new LinkResolverRequestMessage();
        this.setCommons(request);
        request.setLinks(Iterables.toArray(items, LinkDTO.class));

        return request;
    }

    @Override
    public LinkResolverRequestMessage map(LinkedTweetDTO tweet) throws Exception {
        List<LinkDTO> links = new ArrayList<>();
        for (LinkedEntityDTO entity : tweet.getEntities()) {
            links.add(new LinkDTO(entity.getLink(), tweet.getId()));
        }

        LOG.debug("Starting link resolver processing {} links",links.size());

        return this.buildRequest(links);
    }
}