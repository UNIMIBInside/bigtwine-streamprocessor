package it.unimib.disco.bigtwine.streamprocessor.request;

import it.unimib.disco.bigtwine.commons.messaging.RequestMessage;

import java.io.Serializable;
import java.util.Random;

public abstract class AbstractRequestMessageBuilder<T extends RequestMessage, M> implements Serializable {
    protected String outputTopic;
    protected String requestIdPrefix;

    public AbstractRequestMessageBuilder() {
    }

    public AbstractRequestMessageBuilder(String outputTopic, String requestIdPrefix) {
        this.outputTopic = outputTopic;
        this.requestIdPrefix = requestIdPrefix;
    }

    protected abstract T buildRequest(Iterable<M> items);

    protected void setCommons(T request) {
        request.setRequestId(String.format("%s%d", this.requestIdPrefix, new Random().nextLong()));
        request.setOutputTopic(outputTopic);
    }

    public String getOutputTopic() {
        return outputTopic;
    }

    public String getRequestIdPrefix() {
        return requestIdPrefix;
    }

    public void setOutputTopic(String outputTopic) {
        this.outputTopic = outputTopic;
    }

    public void setRequestIdPrefix(String requestIdPrefix) {
        this.requestIdPrefix = requestIdPrefix;
    }
}
