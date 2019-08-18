package it.unimib.disco.bigtwine.streamprocessor;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

public class DatasetProgressWindowFunction implements AllWindowFunction<Tuple2<Integer, Integer>, Tuple2<Double, Boolean>, GlobalWindow> {

    private final long timeout;
    private long lastChangeTs = -1;
    private long tweets = 0;
    private long processedTweets = 0;
    private boolean datasetCompleted = false;

    public static DatasetProgressWindowFunction create(Time timeout) {
        return new DatasetProgressWindowFunction(timeout);
    }

    private DatasetProgressWindowFunction(Time timeout) {
        this.timeout = timeout.toMilliseconds();
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

        double progress = (processedTweets == 0) ? 0.0 : (processedTweets / (double)tweets);
        boolean isLast = datasetCompleted && ((progress == 1.0) || (((System.currentTimeMillis() - lastChangeTs) > timeout)));

        out.collect(new Tuple2<>(progress, isLast));
    }
}
