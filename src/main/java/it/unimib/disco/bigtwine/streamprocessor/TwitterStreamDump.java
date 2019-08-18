package it.unimib.disco.bigtwine.streamprocessor;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.util.Properties;

public class TwitterStreamDump {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // ----- TWITTER STREAM SOURCE
        final String twitterToken = "96366271-uA7vHwZkeXSI7iJa0jHRUO68xEi7qG3TmF1Z44pJX";
        final String twitterTokenSecret = "ZuZqAoOrREHGg2P9TkhFjnZEAWhqfQ2Mx7CLUYpXCj2gB";
        final String twitterConsumerKey = "K1RqX1M82afyenoSYxXgaKKpu";
        final String twitterConsumerSecret = "zKzoBATHoanWi5XNDntwDn769j8Cx5yQPRvBvqxdq5Kys7iyXo";
        final String[] twitterStreamQueryTerms = new String[]{"google", "apple", "microsoft"};
        final String[] twitterStreamLangs = new String[]{"en"};

        Properties twitterProps = new Properties();
        twitterProps.setProperty(TwitterSource.CONSUMER_KEY, twitterConsumerKey);
        twitterProps.setProperty(TwitterSource.CONSUMER_SECRET, twitterConsumerSecret);
        twitterProps.setProperty(TwitterSource.TOKEN, twitterToken);
        twitterProps.setProperty(TwitterSource.TOKEN_SECRET, twitterTokenSecret);
        TwitterSource twitterSource = new TwitterSource(twitterProps);
        twitterSource.setCustomEndpointInitializer(new FilterableTwitterEndpointInitializer(twitterStreamQueryTerms, twitterStreamLangs));

        DataStream<Tuple4<String, String, String, String>> tweetsStream = env
                .addSource(twitterSource)
                .flatMap((String tweetJson, Collector<Status> collector) -> {
                    try {
                        Status tweet = TwitterObjectFactory.createStatus(tweetJson);
                        if (tweet.getId() > 0 && tweet.getText() != null && !tweet.getText().isEmpty()) {
                            collector.collect(tweet);
                        }
                    } catch (TwitterException e) { e.printStackTrace(); }
                })
                .returns(Status.class)
                .map((status) -> {
                    /*
                    Map<String, String> tweet = new HashMap<>();
                    tweet.put("id", String.valueOf(status.getId()));
                    tweet.put("text", status.getText());
                    tweet.put("user__id", String.valueOf(status.getUser().getId()));
                    tweet.put("user__name", status.getUser().getName());
                    */

                    return new Tuple4<>(
                            String.valueOf(status.getId()),
                            status.getText().replace("\n", "").replace("\r", ""),
                            String.valueOf(status.getUser().getId()),
                            status.getUser().getScreenName()
                    );
                })
                .returns(new TypeHint<Tuple4<String, String, String, String>>() {});

        tweetsStream
                .writeAsCsv("/Users/fausto/Desktop/tweets.csv", FileSystem.WriteMode.NO_OVERWRITE, "\n", "\t")
                .setParallelism(1);

        env.execute();
    }
}
