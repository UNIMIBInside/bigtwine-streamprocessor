package it.unimib.disco.bigtwine.streamprocessor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import it.unimib.disco.bigtwine.commons.messaging.*;

import it.unimib.disco.bigtwine.commons.models.*;
import it.unimib.disco.bigtwine.commons.models.dto.*;
import it.unimib.disco.bigtwine.streamprocessor.request.GeoDecoderRequestMessageBuilder;
import it.unimib.disco.bigtwine.streamprocessor.request.LinkResolverRequestMessageBuilder;
import it.unimib.disco.bigtwine.streamprocessor.request.NelRequestMessageBuilder;
import it.unimib.disco.bigtwine.streamprocessor.request.NerRequestMessageBuilder;
import it.unimib.disco.bigtwine.streamprocessor.request.serializer.RequestMessageSerializer;
import it.unimib.disco.bigtwine.streamprocessor.response.GeoDecoderResponseMessageParser;
import it.unimib.disco.bigtwine.streamprocessor.response.LinkResolverResponseMessageParser;
import it.unimib.disco.bigtwine.streamprocessor.response.NelResponseMessageParser;
import it.unimib.disco.bigtwine.streamprocessor.response.NerResponseMessageParser;
import it.unimib.disco.bigtwine.streamprocessor.source.GridFSCsvSource;
import org.apache.commons.lang.BooleanUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;


import java.time.Instant;
import java.util.*;


public class TwitterStreamJob {
    private static final Logger LOG = LoggerFactory.getLogger(TwitterStreamJob.class);

    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        final String jobId = parameters.getRequired("job-id");
        final String analysisId = parameters.getRequired("analysis-id");
        final int heartbeatInterval = parameters.getInt("heartbeat-interval", -1);
        final boolean twitterSkipRetweets = parameters.getBoolean("twitter-skip-retweets", false);
        final String twitterStreamQuery = parameters.get("twitter-stream-query");
        final String twitterStreamLocations = parameters.get("twitter-stream-locations");
        final String datasetDocumentId = parameters.get("dataset-document-id");

        final boolean isQueryInput = twitterStreamQuery != null && !StringUtils.isNullOrWhitespaceOnly(twitterStreamQuery);
        final boolean isLocationsInput = twitterStreamLocations != null && !StringUtils.isNullOrWhitespaceOnly(twitterStreamLocations);
        final boolean isDatasetInput = datasetDocumentId != null && !StringUtils.isNullOrWhitespaceOnly(datasetDocumentId);
        final boolean isStreamMode = isQueryInput || isLocationsInput;

        int providedInputCount = 0;
        providedInputCount += BooleanUtils.toInteger(isQueryInput);
        providedInputCount += BooleanUtils.toInteger(isLocationsInput);
        providedInputCount += BooleanUtils.toInteger(isDatasetInput);
        
        Preconditions.checkArgument(
                providedInputCount == 1,
                "Input not provided or multiple inputs provided" +
                "only one of {} must be provided",
                String.join(", ", new String[]{"twitter-stream-query", "twitter-stream-locations", "dataset-document-id"}));

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();

        DataStream<Tuple3<Double, Boolean, Map<String, String>>> datasetStream = null;
        DataStream<String> rawTweetsStream;

        if (isStreamMode) {
            final String twitterToken = parameters.getRequired("twitter-token");
            final String twitterTokenSecret = parameters.getRequired("twitter-token-secret");
            final String twitterConsumerKey = parameters.getRequired("twitter-consumer-key");
            final String twitterConsumerSecret = parameters.getRequired("twitter-consumer-secret");
            final String twitterStreamLang = parameters.get("twitter-stream-lang", "en");
            final int twitterStreamSampling = parameters.getInt("twitter-stream-sampling", -1);
            final String[] twitterStreamLangs = twitterStreamLang.split(",");

            LOG.debug("Stream query: {}", twitterStreamQuery);

            // ----- TWITTER STREAM SOURCE
            Properties twitterProps = new Properties();
            twitterProps.setProperty(TwitterSource.CONSUMER_KEY, twitterConsumerKey);
            twitterProps.setProperty(TwitterSource.CONSUMER_SECRET, twitterConsumerSecret);
            twitterProps.setProperty(TwitterSource.TOKEN, twitterToken);
            twitterProps.setProperty(TwitterSource.TOKEN_SECRET, twitterTokenSecret);
            TwitterSource twitterSource = new TwitterSource(twitterProps);

            if (isQueryInput) {
                final String[] twitterStreamQueryTerms = twitterStreamQuery.split(",");
                twitterSource.setCustomEndpointInitializer(new FilterableTwitterEndpointInitializer(twitterStreamQueryTerms, twitterStreamLangs));
            } else {
                twitterSource.setCustomEndpointInitializer(new FilterableTwitterEndpointInitializer(twitterStreamLocations, twitterStreamLangs));
            }

            rawTweetsStream = env
                    .addSource(twitterSource)
                    .setMaxParallelism(1)
                    .name("Tweets source");

            if (twitterStreamSampling > 0) {
                rawTweetsStream = rawTweetsStream
                        .filter(new TwitterStatusSamplingFilter(twitterStreamSampling))
                        .setMaxParallelism(1)
                        .name("Tweets sampling");

                LOG.debug("Sampling enabled: {} tweets per seconds", twitterStreamSampling);
            }
        } else if (isDatasetInput) {
            GridFSCsvSource gridFsSource = new GridFSCsvSource(
                    Constants.GRIDFS_HOST,
                    Constants.GRIDFS_PORT,
                    Constants.GRIDFS_DB,
                    new ObjectId(datasetDocumentId));

            datasetStream = env
                    .addSource(gridFsSource)
                    .setMaxParallelism(1)
                    .name("Dataset source");

            rawTweetsStream = datasetStream
                    .map(t -> t.f2)
                    .map(new MapToJsonSerializer())
                    .setMaxParallelism(1)
                    .name("Tweets source");
        } else {
            throw new RuntimeException("Missing input");
        }

        DataStream<Status> tweetsStream = rawTweetsStream
                .flatMap((String tweetJson, Collector<Status> collector) -> {
                    try {
                        Status tweet = TwitterObjectFactory.createStatus(tweetJson);
                        if (tweet.getId() > 0 && tweet.getText() != null && !tweet.getText().isEmpty()) {
                            if (!(twitterSkipRetweets && tweet.isRetweet())) {
                                collector.collect(tweet);
                            }
                        }
                    } catch (TwitterException e) {
                        LOG.debug("Tweet not parsed - {}: {}", e.getMessage(), tweetJson);
                    }
                })
                .returns(Status.class)
                .name("Tweets parsing");

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "kafka:9092");
        kafkaProps.setProperty("group.id", "test");

        final String nerInputTopic = Constants.NER_INPUT_TOPIC;
        final String nerOutputTopic = String.format(Constants.NER_OUTPUT_TOPIC, analysisId);
        final String nerRecognizer = parameters.get("ner-recognizer", null);
        FlinkKafkaProducer<String> nerSink = new FlinkKafkaProducer<>(nerInputTopic, new SimpleStringSchema(), kafkaProps);
        FlinkKafkaConsumer<String> nerSource = new FlinkKafkaConsumer<>(nerOutputTopic, new SimpleStringSchema(), kafkaProps);

        tweetsStream
                .map(status -> new BasicTweetDTO(String.valueOf(status.getId()), status.getText()))
                .timeWindowAll(Time.seconds(3))
                .apply(new NerRequestMessageBuilder(nerOutputTopic, "ner-request-", nerRecognizer))
                .map(new RequestMessageSerializer<>(nerInputTopic))
                .addSink(nerSink)
                .name("NER sink");

        DataStream<RecognizedTweetDTO> recognizedTweetsStream = env
                .addSource(nerSource)
                .flatMap(new NerResponseMessageParser(nerOutputTopic));

        final String nelInputTopic = Constants.NEL_INPUT_TOPIC;
        final String nelOutputTopic = String.format(Constants.NEL_OUTPUT_TOPIC, analysisId);
        final String nelLinker = parameters.get("nel-linker", null);
        FlinkKafkaProducer<String> nelSink = new FlinkKafkaProducer<>(nelInputTopic, new SimpleStringSchema(), kafkaProps);
        FlinkKafkaConsumer<String> nelSource = new FlinkKafkaConsumer<>(nelOutputTopic, new SimpleStringSchema(), kafkaProps);

        recognizedTweetsStream
                .timeWindowAll(Time.seconds(3))
                .apply(new NelRequestMessageBuilder(nelOutputTopic, "nel-request-", nelLinker))
                .map(new RequestMessageSerializer<>(nelInputTopic))
                .addSink(nelSink)
                .name("NEL sink");

        DataStream<LinkedTweetDTO> linkedTweetsStream = env
                .addSource(nelSource)
                .flatMap(new NelResponseMessageParser(nelOutputTopic));

        final String linkResolverInputTopic = Constants.LINKRESOLVER_INPUT_TOPIC;
        final String linkResolverOutputTopic = String.format(Constants.LINKRESOLVER_OUTPUT_TOPIC, analysisId);
        FlinkKafkaProducer<String> linkSink = new FlinkKafkaProducer<>(linkResolverInputTopic, new SimpleStringSchema(), kafkaProps);
        FlinkKafkaConsumer<String> linkSource = new FlinkKafkaConsumer<>(linkResolverOutputTopic, new SimpleStringSchema(), kafkaProps);

        linkedTweetsStream
                .filter(tweet -> {
                    int count = 0;
                    for (LinkedEntity entity : tweet.getEntities()) {
                        if (entity.getLink() != null) {
                            count++;
                        }
                    }

                    return count > 0;
                })
                .map(new LinkResolverRequestMessageBuilder(linkResolverOutputTopic, "linkresolver-request-"))
                .map(new RequestMessageSerializer<>(linkResolverInputTopic))
                .addSink(linkSink)
                .name("Link resolver sink");

        DataStream<List<ResourceDTO>> resourcesStream = env
                .addSource(linkSource)
                .flatMap(new LinkResolverResponseMessageParser(linkResolverOutputTopic));

        final String geoDecoderInputTopic = Constants.GEODECODER_INPUT_TOPIC;
        final String geoDecoderOutputTopic = String.format(Constants.GEODECODER_OUTPUT_TOPIC, analysisId);
        final String geoDecoder = parameters.get("geo-decoder", "default");
        FlinkKafkaProducer<String> geoSink = new FlinkKafkaProducer<>(geoDecoderInputTopic, new SimpleStringSchema(), kafkaProps);
        FlinkKafkaConsumer<String> geoSource = new FlinkKafkaConsumer<>(geoDecoderOutputTopic, new SimpleStringSchema(), kafkaProps);

        tweetsStream
                .map(status -> new LocationDTO(status.getUser().getLocation(), String.valueOf(status.getId())))
                .filter(loc -> loc.getTag() != null && loc.getAddress() != null && !loc.getAddress().isEmpty())
                .timeWindowAll(Time.seconds(3))
                .apply(new GeoDecoderRequestMessageBuilder(geoDecoderOutputTopic, "geodecoder-reqeust-", geoDecoder))
                .map(new RequestMessageSerializer<>(geoDecoderInputTopic))
                .addSink(geoSink)
                .name("Geo decoder sink");

        DataStream<DecodedLocationDTO> locationsStream = env
                .addSource(geoSource)
                .flatMap(new GeoDecoderResponseMessageParser(geoDecoderOutputTopic));

        DataStream<Tuple3<String, Object, StreamType>> tupleTweetsStream = tweetsStream
                .filter((tweet) -> tweet != null && tweet.getId() > 0)
                .map(tweet -> new Tuple3<>(String.valueOf(tweet.getId()), (Object)tweet, StreamType.status))
                .returns(new TypeHint<Tuple3<String, Object, StreamType>>(){})
                .name("Raw tweet tuple mapper");

        DataStream<Tuple3<String, Object, StreamType>> tupleLinkedTweetsStream = linkedTweetsStream
                .filter((tweet) -> tweet != null && tweet.getId() != null)
                .map(linkedTweet -> new Tuple3<>(String.valueOf(linkedTweet.getId()), (Object)linkedTweet, StreamType.linkedTweet))
                .returns(new TypeHint<Tuple3<String, Object, StreamType>>(){})
                .name("Linked tweet tuple mapper");

        DataStream<Tuple3<String, Object, StreamType>> tupleResourcesStream = resourcesStream
                .filter((resources) -> resources != null && resources.size() > 0 && resources.get(0).getTag() != null)
                .map(resources -> new Tuple3<>(resources.get(0).getTag(), (Object)resources, StreamType.resource))
                .returns(new TypeHint<Tuple3<String, Object, StreamType>>(){})
                .name("Resource tuple mapper");

        DataStream<Tuple3<String, Object, StreamType>> tupleLocationsStream = locationsStream
                .filter((loc) -> loc != null && loc.getTag() != null)
                .map(location -> new Tuple3<>(location.getTag(), (Object)location, StreamType.decodedLocation))
                .returns(new TypeHint<Tuple3<String, Object, StreamType>>(){})
                .name("Location tuple mapper");

        DataStream<NeelProcessedTweetDTO> processedTweetsStream = tupleTweetsStream
                .union(tupleLinkedTweetsStream, tupleResourcesStream, tupleLocationsStream)
                .keyBy(0)
                .window(GlobalWindows.create())
                .trigger(TwitterStreamTypeWindowTrigger.create(Time.seconds(15)))
                .apply(new NeelProcessedTweetWindowFunction())
                .name("Neel processed tweet assembler");

        FlinkKafkaProducer<String> tweetsProcessedProducer = new FlinkKafkaProducer<>("analysis-results", new SimpleStringSchema(), kafkaProps);

        processedTweetsStream
                .map((tweet) -> {
                    AnalysisResultProducedEvent event = new AnalysisResultProducedEvent();
                    event.setAnalysisId(analysisId);
                    event.setProcessDate(Instant.now());
                    event.setPayload(tweet);

                    LOG.debug("Analysis result produced for tweet: {}", tweet.getStatus().getId());

                    return event;
                })
                .map(event -> {
                    ObjectMapper mapper = new ObjectMapper();
                    mapper.registerModule(new JavaTimeModule());
                    JsonSerializer<AnalysisResultProducedEvent> serializer = new JsonSerializer<>(mapper);

                    return new String(serializer.serialize("analysis-results", event));
                })
                .addSink(tweetsProcessedProducer)
                .name("NEEL Output Sink");

        if (heartbeatInterval > 0) {
            FlinkKafkaProducer<String> heartbeatSink = new FlinkKafkaProducer<>("job-heartbeats", new SimpleStringSchema(), kafkaProps);
            DataStream<JobHeartbeatEvent> heartbeatStream;

            if (datasetStream == null) {
                heartbeatStream = tweetsStream
                        .map(t -> 1)
                        .timeWindowAll(Time.seconds(heartbeatInterval))
                        .reduce(Integer::sum)
                        .map(count -> {
                            JobHeartbeatEvent event = new JobHeartbeatEvent();
                            event.setTimestamp(Instant.now());
                            event.setJobId(jobId);

                            return event;
                        });

            } else {
                heartbeatStream = datasetStream
                        .map(t -> new Tuple2<>(t.f0, t.f1))
                        .timeWindowAll(Time.seconds(heartbeatInterval))
                        .maxBy(0)
                        .map(t -> {
                            JobHeartbeatEvent event = new JobHeartbeatEvent();
                            event.setTimestamp(Instant.now());
                            event.setJobId(jobId);
                            event.setProgress(t.f0);
                            event.setLast(t.f1);

                            return event;
                        });
            }

            heartbeatStream
                    .map(event -> {
                        ObjectMapper mapper = new ObjectMapper();
                        mapper.registerModule(new JavaTimeModule());
                        JsonSerializer<JobHeartbeatEvent> serializer = new JsonSerializer<>(mapper);

                        return new String(serializer.serialize("job-heartbeats", event));
                    })
                    .addSink(heartbeatSink);
        }

        env.execute();
    }

}
