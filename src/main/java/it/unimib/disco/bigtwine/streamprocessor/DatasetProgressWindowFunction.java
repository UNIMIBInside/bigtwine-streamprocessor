package it.unimib.disco.bigtwine.streamprocessor;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatasetProgressWindowFunction implements AllWindowFunction<Tuple2<Integer, Integer>, Tuple2<Double, Boolean>, GlobalWindow> {

    private static final Logger LOG = LoggerFactory.getLogger(DatasetProgressWindowFunction.class);

    private final long timeout;
    private long lastChangeTs = -1;
    private long tweets = 0;
    private long processedTweets = 0;
    private boolean datasetCompleted = false;
    private int expNumberOfRecords = -1;

    public static DatasetProgressWindowFunction create(Time timeout, int expNumberOfRecords) {
        return new DatasetProgressWindowFunction(timeout, expNumberOfRecords);
    }

    public static DatasetProgressWindowFunction create(Time timeout) {
        return create(timeout, -1);
    }

    private DatasetProgressWindowFunction(Time timeout, int expNumberOfRecords) {
        this.timeout = timeout.toMilliseconds();
        this.expNumberOfRecords = expNumberOfRecords;
    }

    @Override
    public void apply(GlobalWindow window, Iterable<Tuple2<Integer, Integer>> values, Collector<Tuple2<Double, Boolean>> out) throws Exception {
        for (Tuple2<Integer, Integer> value : values) {
            switch (value.f0) {
                case 1:
                    if (!datasetCompleted) {
                        datasetCompleted = value.f1 == 1;
                    }
                    break;
                case 2:
                    tweets += value.f1;
                    break;
                case 3:
                    processedTweets += value.f1;
                    break;
                default:
                    break;
            }

            if (value.f0 != 0) {
                lastChangeTs = System.currentTimeMillis();
            }
        }

        double progress;
        if (expNumberOfRecords > 0 && expNumberOfRecords > tweets) {
            progress = (processedTweets / (double)expNumberOfRecords);
        } else if (tweets == 0) {
            progress = 0;
        } else {
            progress = processedTweets / (double)tweets;
        }

        boolean isLast = datasetCompleted && ((progress == 1.0) || (((System.currentTimeMillis() - lastChangeTs) > timeout)));

        LOG.info("Dataset progress {} (last {}) - {}, {}, {}, {}", (int)(progress * 100), isLast,
                datasetCompleted, expNumberOfRecords, processedTweets, tweets);

        out.collect(new Tuple2<>(progress, isLast));
    }
}
