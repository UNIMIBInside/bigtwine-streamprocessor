package it.unimib.disco.bigtwine.streamprocessor;

import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;

import java.io.Serializable;
import java.util.Arrays;

class FilterableTwitterEndpointInitializer implements TwitterSource.EndpointInitializer, Serializable {

    private String[] terms;
    private String[] langs;

    public FilterableTwitterEndpointInitializer() {
    }

    public FilterableTwitterEndpointInitializer(String[] terms) {
        this.terms = terms;
    }

    public FilterableTwitterEndpointInitializer(String[] terms, String[] langs) {
        this.terms = terms;
        this.langs = langs;
    }

    @Override
    public StreamingEndpoint createEndpoint() {
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();

        if (this.terms != null) {
            endpoint.trackTerms(Arrays.asList(this.terms));
        }
        if (this.langs != null) {
            endpoint.languages(Arrays.asList(this.langs));
        }

        return endpoint;
    }

    public String[] getTerms() {
        return terms;
    }

    public void setTerms(String[] terms) {
        this.terms = terms;
    }

    public String[] getLangs() {
        return langs;
    }

    public void setLangs(String[] langs) {
        this.langs = langs;
    }
}
