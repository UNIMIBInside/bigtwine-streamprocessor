package it.unimib.disco.bigtwine.streamprocessor;

import com.google.common.collect.Iterables;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import it.unimib.disco.bigtwine.commons.messaging.*;

import it.unimib.disco.bigtwine.commons.models.*;
import it.unimib.disco.bigtwine.commons.models.dto.*;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;


import java.io.Serializable;
import java.time.Instant;
import java.util.*;

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

enum StreamType implements Serializable {
    status, linkedTweet, resource, decodedLocation
}

class AnalysisIdValidator {
    public final String legalChars = "[a-zA-Z0-9\\-]+";
    public final int maxNameLength = 160;

    public boolean validate(String analysisId) throws IllegalArgumentException {
        if (analysisId == null) {
            throw new IllegalArgumentException("analysis id must not be null");
        }

        if (analysisId.isEmpty() || analysisId.length() > maxNameLength) {
            throw new IllegalArgumentException("analysis id must not be empty and must shorter than " + maxNameLength + " chars");
        }

        if (!analysisId.matches(legalChars)) {
            throw new IllegalArgumentException("analysis id must contains only letters, numbers and minus");
        }

        return true;
    }
}

public class TwitterStreamJob {
    private static final Logger LOG = LoggerFactory.getLogger(TwitterStreamJob.class);

    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        final String jobId = parameters.get("job-id");
        final String analysisId = parameters.get("analysis-id");
        final String twitterToken = parameters.get("twitter-token");
        final String twitterTokenSecret = parameters.get("twitter-token-secret");
        final String twitterConsumerKey = parameters.get("twitter-consumer-key");
        final String twitterConsumerSecret = parameters.get("twitter-consumer-secret");
        final String twitterStreamQuery = parameters.get("twitter-stream-query", "apple,iphone,ipad,ios,android,samsung");
        final String twitterStreamLang = parameters.get("twitter-stream-lang", "en");
        final int heartbeatInterval = parameters.getInt("heartbeat-interval", -1);
        final int twitterStreamSampling = parameters.getInt("twitter-stream-sampling", -1);
        final String[] twitterStreamQueryTerms = twitterStreamQuery.split(",");
        final String[] twitterStreamLangs = twitterStreamLang.split(",");

        // --twitter-token 96366271-uA7vHwZkeXSI7iJa0jHRUO68xEi7qG3TmF1Z44pJX \
        // --twitter-token-secret ZuZqAoOrREHGg2P9TkhFjnZEAWhqfQ2Mx7CLUYpXCj2gB \
        // --twitter-consumer-key K1RqX1M82afyenoSYxXgaKKpu \
        // --twitter-consumer-secret zKzoBATHoanWi5XNDntwDn769j8Cx5yQPRvBvqxdq5Kys7iyXo

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // ----- TWITTER STREAM SOURCE
        Properties twitterProps = new Properties();
        twitterProps.setProperty(TwitterSource.CONSUMER_KEY, twitterConsumerKey);
        twitterProps.setProperty(TwitterSource.CONSUMER_SECRET, twitterConsumerSecret);
        twitterProps.setProperty(TwitterSource.TOKEN, twitterToken);
        twitterProps.setProperty(TwitterSource.TOKEN_SECRET, twitterTokenSecret);
        TwitterSource twitterSource = new TwitterSource(twitterProps);
        twitterSource.setCustomEndpointInitializer(new FilterableTwitterEndpointInitializer(twitterStreamQueryTerms, twitterStreamLangs));

        DataStream<Status> tweetsStream = env
                .addSource(twitterSource)
                .map(tweetJson -> {
                    try {
                        Status tweet = TwitterObjectFactory.createStatus(tweetJson);
                        if (tweet.getId() > 0 && tweet.getText() != null && !tweet.getText().isEmpty()) {
                            return tweet;
                        }else {
                            return null;
                        }
                    }catch (TwitterException e) {
                        return null;
                    }
                })
                .filter(t -> t != null);

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "localhost:9092");
        // only required for Kafka 0.8
        kafkaProps.setProperty("zookeeper.connect", "localhost:2181");
        kafkaProps.setProperty("group.id", "test");

        FlinkKafkaProducer<String> nerProducer = new FlinkKafkaProducer<>("ner-requests", new SimpleStringSchema(), kafkaProps);
        FlinkKafkaConsumer<String> nerConsumer = new FlinkKafkaConsumer<>("ner-responses." + analysisId, new SimpleStringSchema(), kafkaProps);

        tweetsStream
                .map(status -> (BasicTweet)new BasicTweetDTO(String.valueOf(status.getId()), status.getText()))
                .timeWindowAll(Time.seconds(3))
                .apply(new AllWindowFunction<BasicTweet, NerRequestMessage, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<BasicTweet> tweets, Collector<NerRequestMessage> out) throws Exception {
                        if (!tweets.iterator().hasNext()) {
                            return;
                        }

                        NerRequestMessage request = new NerRequestMessage();
                        request.setRequestId("test-request-" + new Random().nextLong());
                        request.setRecognizer("test");
                        request.setOutputTopic("ner-responses." + analysisId);
                        request.setTweets(Iterables.toArray(tweets, BasicTweet.class));

                        out.collect(request);
                    }
                })
                .map(request -> new String(new JsonSerializer<>().serialize("ner-requests", request)))
                .addSink(nerProducer);

        FlinkKafkaProducer<String> nelProducer = new FlinkKafkaProducer<>("nel-requests", new SimpleStringSchema(), kafkaProps);
        FlinkKafkaConsumer<String> nelConsumer = new FlinkKafkaConsumer<>("nel-responses." + analysisId, new SimpleStringSchema(), kafkaProps);

        DataStream<RecognizedTweet> recognizedTweetsStream = env
                .addSource(nerConsumer)
                .flatMap(new FlatMapFunction<String, RecognizedTweet>() {
                    @Override
                    public void flatMap(String json, Collector<RecognizedTweet> out) throws Exception {
                        JsonDeserializer<NerResponseMessage> deserializer = new JsonDeserializer<>(NerResponseMessage.class);
                        NerResponseMessage res = deserializer.deserialize("ner-responses." + analysisId, json.getBytes());
                        for (RecognizedTweet tweet : res.getTweets())  {
                            out.collect(tweet);
                        }
                    }
                });

        recognizedTweetsStream
                .timeWindowAll(Time.seconds(3))
                .apply(new AllWindowFunction<RecognizedTweet, NelRequestMessage, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<RecognizedTweet> tweets, Collector<NelRequestMessage> out) throws Exception {
                        NelRequestMessage request = new NelRequestMessage();
                        request.setRequestId("test-request-" + new Random().nextLong());
                        request.setOutputTopic("nel-responses." + analysisId);
                        request.setLinker("test");
                        request.setTweets(Iterables.toArray(tweets, RecognizedTweet.class));

                        out.collect(request);
                    }
                })
                .map(request -> new String(new JsonSerializer<>().serialize("nel-requests", request)))
                .addSink(nelProducer);

        DataStream<LinkedTweet> linkedTweetsStream = env
                .addSource(nelConsumer)
                .flatMap(new FlatMapFunction<String, LinkedTweet>() {
                    @Override
                    public void flatMap(String json, Collector<LinkedTweet> out) throws Exception {
                        JsonDeserializer<NelResponseMessage> deserializer = new JsonDeserializer<>(NelResponseMessage.class);
                        NelResponseMessage res = deserializer.deserialize("nel-responses." + analysisId, json.getBytes());
                        for (LinkedTweet tweet : res.getTweets())  {
                            out.collect(tweet);
                        }
                    }
                });

        FlinkKafkaProducer<String> linkProducer = new FlinkKafkaProducer<>("linkresolver-requests", new SimpleStringSchema(), kafkaProps);
        FlinkKafkaConsumer<String> linkConsumer = new FlinkKafkaConsumer<>("linkresolver-responses." + analysisId, new SimpleStringSchema(), kafkaProps);


        linkedTweetsStream
                .flatMap(new FlatMapFunction<LinkedTweet, Link>() {
                    @Override
                    public void flatMap(LinkedTweet tweet, Collector<Link> out) throws Exception {
                        for (LinkedEntity entity : tweet.getEntities()) {
                            out.collect(new LinkDTO(entity.getLink(), tweet.getId()));
                        }
                    }
                })
                .map(link -> {
                    LinkResolverRequestMessage request = new LinkResolverRequestMessage();
                    request.setRequestId("test-request-" + new Random().nextLong());
                    request.setOutputTopic("linkresolver-responses." + analysisId);
                    request.setLinks(new Link[]{link});

                    return request;
                })
                .map(request -> new String(new JsonSerializer<>().serialize("linkresolver-requests", request)))
                .addSink(linkProducer);

        DataStream<Resource> resourcesStream = env
                .addSource(linkConsumer)
                .flatMap(new FlatMapFunction<String, Resource>() {
                    @Override
                    public void flatMap(String json, Collector<Resource> out) throws Exception {
                        JsonDeserializer<LinkResolverResponseMessage> deserializer = new JsonDeserializer<>(LinkResolverResponseMessage.class);
                        LinkResolverResponseMessage res = deserializer.deserialize("linkresolver-responses." + analysisId, json.getBytes());
                        for (Resource resource : res.getResources())  {
                            out.collect(resource);
                        }
                    }
                });


        FlinkKafkaProducer<String> geoProducer = new FlinkKafkaProducer<>("geodecoder-requests", new SimpleStringSchema(), kafkaProps);
        FlinkKafkaConsumer<String> geoConsumer = new FlinkKafkaConsumer<>("geodecoder-responses." + analysisId, new SimpleStringSchema(), kafkaProps);

        tweetsStream
                .map(status -> new LocationDTO(status.getUser().getLocation(), String.valueOf(status.getId())))
                .filter(loc -> loc.getTag() != null && loc.getAddress() != null && !loc.getAddress().isEmpty())
                .map((location) -> {
                    GeoDecoderRequestMessage request = new GeoDecoderRequestMessage();
                    request.setRequestId("test-request-" + new Random().nextLong());
                    request.setOutputTopic("geodecoder-responses." + analysisId);
                    request.setLocations(new Location[]{location});

                    return request;
                })
                .map(request -> new String(new JsonSerializer<>().serialize("geodecoder-requests", request)))
                .addSink(geoProducer);

        DataStream<DecodedLocation> locationsStream = env
                .addSource(geoConsumer)
                .flatMap(new FlatMapFunction<String, DecodedLocation>() {
                    @Override
                    public void flatMap(String json, Collector<DecodedLocation> out) throws Exception {
                        JsonDeserializer<GeoDecoderResponseMessage> deserializer = new JsonDeserializer<>(GeoDecoderResponseMessage.class);
                        GeoDecoderResponseMessage res = deserializer.deserialize("geodecoder-responses." + analysisId, json.getBytes());
                        for (DecodedLocation location : res.getLocations())  {
                            out.collect(location);
                        }
                    }
                });

        DataStream<Tuple2<String, Object>> tupleTweetsStream = tweetsStream
                .map(new MapFunction<Status, Tuple2<String, Object>>() {
                    @Override
                    public Tuple2<String, Object> map(Status tweet) throws Exception {
                        return new Tuple2<>(String.valueOf(tweet.getId()), tweet);
                    }
                });

        DataStream<Tuple2<String, Object>> tupleLinkedTweetsStream = linkedTweetsStream
                .map(new MapFunction<LinkedTweet, Tuple2<String, Object>>() {
                    @Override
                    public Tuple2<String, Object> map(LinkedTweet tweet) throws Exception {
                        return new Tuple2<>(String.valueOf(tweet.getId()), tweet);
                    }
                });

        DataStream<Tuple2<String, Object>> tupleResourcesStream = resourcesStream
                .map(new MapFunction<Resource, Tuple2<String, Object>>() {
                    @Override
                    public Tuple2<String, Object> map(Resource resource) throws Exception {
                        return new Tuple2<>(resource.getTag(), resource);
                    }
                });

        DataStream<Tuple2<String, Object>> tupleLocationsStream = locationsStream
                .map(new MapFunction<DecodedLocation, Tuple2<String, Object>>() {
                    @Override
                    public Tuple2<String, Object> map(DecodedLocation location) throws Exception {
                        return new Tuple2<>(location.getTag(), location);
                    }
                });

        DataStream<NeelProcessedTweetDTO> processedTweetsStream = tupleTweetsStream
                .union(tupleLinkedTweetsStream, tupleResourcesStream, tupleLocationsStream)
                .keyBy(0)
                .timeWindow(Time.seconds(15))
                .apply(new WindowFunction<Tuple2<String, Object>, NeelProcessedTweetDTO, Tuple, TimeWindow>() {

                    private <TI> TI deserialize(String value, Class<TI> type) {
                        byte[] bytes = value.getBytes();
                        return SerializationUtils.deserialize(bytes);
                    }

                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Object>> input, Collector<NeelProcessedTweetDTO> out) throws Exception {
                        NeelProcessedTweetDTO tweet = new NeelProcessedTweetDTO();
                        TwitterStatus status = new TwitterStatusDTO();
                        TwitterUser user = new TwitterUserDTO();
                        List<LinkedEntity> entities = new ArrayList<>();
                        Map<String, Resource> resourcesMap = new HashMap<>();

                        boolean statusReceived = false;
                        boolean entitiesReceived = false;
                        boolean resourcesReceived = false;
                        // boolean locationReceived = false;

                        for (Tuple2<String, Object> t : input) {
                            if (t.f1 instanceof Status) {
                                Status _status = (Status)t.f1;
                                status.setId(String.valueOf(_status.getId()));
                                status.setText(_status.getText());

                                if (_status.getGeoLocation() != null) {
                                    status.setCoordinates(new CoordinatesDTO(
                                            _status.getGeoLocation().getLatitude(),
                                            _status.getGeoLocation().getLongitude()));
                                }

                                user.setId(String.valueOf(_status.getUser().getId()));
                                user.setName(_status.getUser().getName());
                                user.setScreenName(_status.getUser().getScreenName());
                                user.setProfileImageUrl(_status.getUser().getProfileImageURL());
                                user.setLocation(_status.getUser().getLocation());
                                statusReceived = true;
                            }else if (t.f1 instanceof LinkedTweet) {
                                LinkedTweet linkedTweet = (LinkedTweet)t.f1;
                                entities.addAll(Arrays.asList(linkedTweet.getEntities()));
                                entitiesReceived = true;
                            }else if (t.f1 instanceof Resource) {
                                Resource resource = (Resource)t.f1;
                                resourcesMap.put(resource.getUrl(), resource);
                                resourcesReceived = true;
                            }else if (t.f1 instanceof DecodedLocation) {
                                DecodedLocation location = (DecodedLocation)t.f1;
                                user.setCoordinates(location.getCoordinates());
                                // locationReceived = true;
                            }
                        }

                        for (LinkedEntity entity : entities) {
                            if (!entity.isNil() && entity.getLink() != null) {
                                if (resourcesMap.containsKey(entity.getLink())) {
                                    entity.setResource(resourcesMap.get(entity.getLink()));
                                }
                            }

                            if (entity.getValue() == null && entity.getPosition() != null && status.getText() != null) {
                                entity.setValue(status.getText().substring(
                                        entity.getPosition().getStart(),
                                        entity.getPosition().getEnd()));
                            }
                        }

                        status.setUser(user);
                        tweet.setStatus(status);
                        tweet.setEntities(entities.toArray(new LinkedEntity[0]));

                        if (statusReceived && entitiesReceived && resourcesReceived) {
                            out.collect(tweet);
                        }
                    }
                });

        FlinkKafkaProducer<String> tweetsProcessedProducer = new FlinkKafkaProducer<>("analysis-results", new SimpleStringSchema(), kafkaProps);

        processedTweetsStream
                .map((tweet) -> {
                    AnalysisResultProducedEvent event = new AnalysisResultProducedEvent();
                    event.setAnalysisId(analysisId);
                    event.setProcessDate(Instant.now());
                    event.setPayload(tweet);

                    return event;
                })
                .map(event -> new String(new JsonSerializer<>().serialize("analysis-results", event)))
                .addSink(tweetsProcessedProducer);

        processedTweetsStream
                .map(tweet -> "[PROCESSED] " + String.join(", ", new String[]{tweet.getStatus().getId(), tweet.getStatus().getUser().getId(), String.valueOf(tweet.getEntities().length)}))
                .print();


        if (heartbeatInterval > 0) {
            FlinkKafkaProducer<String> heartbeatProducer = new FlinkKafkaProducer<>("job-heartbeats", new SimpleStringSchema(), kafkaProps);

            env.addSource(new SourceFunction<Long>() {
                private boolean shouldStop = false;
                private int counter = 0;

                @Override
                public void run(SourceContext<Long> ctx) throws Exception {
                    while (!shouldStop) {
                        if (counter == heartbeatInterval) {
                            counter = 0;
                            ctx.collect(Instant.now().getEpochSecond());
                        }

                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) { }

                        counter++;
                    }
                }

                @Override
                public void cancel() {
                    shouldStop = true;
                }
            }).map((ts) -> {
                JobHeartbeatEvent event = new JobHeartbeatEvent();
                event.setJobId(jobId);
                event.setTimestamp(Instant.ofEpochSecond(ts));

                return event;
            })
                    .map(event -> new String(new JsonSerializer<>().serialize("job-heartbeats", event)))
                    .addSink(heartbeatProducer);
        }

        env.execute();
    }
}
