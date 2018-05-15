package eu.fbk.fm.vectorize.preprocessing.text;

import com.google.common.base.Preconditions;
import eu.fbk.fm.alignments.utils.flink.RobustTsvOutputFormat;
import eu.fbk.fm.alignments.utils.flink.TextInputFormat;
import eu.fbk.utils.core.CommandLine;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tensorflow.example.*;

import javax.annotation.Nullable;

import java.io.*;
import java.util.*;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.max;
import static java.lang.Math.min;

public class PopulateCooccurrenceMatrix {

    private static final Logger LOGGER = LoggerFactory.getLogger(PopulateCooccurrenceMatrix.class);

    private static final String WORK_PATH = "work-path";
    private static final String WINDOW_SIZE = "window-size";
    private static final String VOCABULARY_SIZE = "vocabulary-size";
    private static final String SHARD_SIZE = "shard-size";
    private static final String SHARD_NUMBER = "shard-number";

    private static final int DEFAULT_WINDOW_SIZE = 10;
    private static final int DEFAULT_VOCABULARY_SIZE = 4000000;
    private static final int DEFAULT_SHARD_SIZE = 4096;

    private int vocabularySize = DEFAULT_VOCABULARY_SIZE;
    private int windowSize = DEFAULT_WINDOW_SIZE;
    private int shardSize = DEFAULT_SHARD_SIZE;

    private DataSet<String> getInput(ExecutionEnvironment env, Path input) {
        return getInput(env, input, new Configuration());
    }

    private DataSet<String> getInput(ExecutionEnvironment env, Path input, Configuration parameters) {
        parameters.setBoolean("recursive.file.enumeration", true);

        return new DataSource<>(
            env,
            new TextInputFormat(input),
            BasicTypeInfo.STRING_TYPE_INFO,
            Utils.getCallLocationName()
        ).withParameters(parameters);
    }

    private static class CooccurrenceExtractor extends RichFlatMapFunction<String, CoocTuple> {

        private Map<String, Integer> dictionary;
        private int windowSize;

        public void open(Configuration cfg) throws Exception {
            this.dictionary = getRuntimeContext()
                .getBroadcastVariableWithInitializer("dictionary",
                    (BroadcastVariableInitializer<Tuple3<Integer, String, Integer>, Map<String, Integer>>) rawDictionary -> {
                        Map<String, Integer> dictionary = new HashMap<>();

                        for (Tuple3<Integer, String, Integer> item : rawDictionary) {
                            dictionary.put(item.f1, item.f0);
                        }

                        LOGGER.info("Loaded "+dictionary.size()+"-word dictionary");
                        return dictionary;
                    }
                );
            this.windowSize = cfg.getInteger(WINDOW_SIZE, DEFAULT_WINDOW_SIZE);
        }

        @Override
        public void flatMap(String value, Collector<CoocTuple> out) throws Exception {
            ArrayList<Integer> sentence = new ArrayList<>();
            for (String token : value.split("\\s+")) {
                if (dictionary.containsKey(token)) {
                    sentence.add(dictionary.get(token));
                }
            }

            for (int i = 0; i < sentence.size(); i++) {
                int leftId = sentence.get(i);
                int windowEnd = min(windowSize + 1, sentence.size() - i);
                for (int offset = 1; offset < windowEnd; offset++) {
                    int rightId = sentence.get(i+offset);
                    float count = 1.0f / offset;
                    out.collect(new CoocTuple(leftId, rightId, count));
                    out.collect(new CoocTuple(rightId, leftId, count));
                }

                out.collect(new CoocTuple(leftId, leftId, 1.0f));
            }
        }
    }

    private static class Zipper extends RichMapPartitionFunction<Tuple2<String, Integer>, Tuple3<Integer, String, Integer>> {

        private int shardSize;

        @Override
        public void open(Configuration parameters) throws Exception {
            shardSize = parameters.getInteger(SHARD_SIZE, DEFAULT_SHARD_SIZE);
            if (getRuntimeContext().getNumberOfParallelSubtasks() > 1) {
                throw new IllegalStateException("The number of parallel tasks for this operator shouldn't exceed 1");
            }
        }

        @Override
        public void mapPartition(Iterable<Tuple2<String, Integer>> values, Collector<Tuple3<Integer, String, Integer>> out) throws Exception {
            int offset = 0;
            LinkedList<Tuple2<String, Integer>> buffer = new LinkedList<>();
            for (Tuple2<String, Integer> dictionaryItem : values) {
                buffer.add(dictionaryItem);
                if (buffer.size() % shardSize == 0) {
                    for (Tuple2<String, Integer> bufferItem : buffer) {
                        out.collect(new Tuple3<>(offset++, bufferItem.f0, bufferItem.f1));
                    }
                    buffer.clear();
                }
            }
        }
    }

    private static class CoocWriter extends RichGroupReduceFunction<CoocTuple, Tuple1<Long>> {

        private String workPath;
        private int shardNumber;
        private int shardSize;

        @Override
        public void open(Configuration parameters) throws Exception {
            workPath = parameters.getString(WORK_PATH, "");
            shardNumber = parameters.getInteger(SHARD_NUMBER, 0);
            shardSize = parameters.getInteger(SHARD_SIZE, 0);
            Preconditions.checkArgument(shardNumber > 0, "The amount of shards should be more than zero");
            Preconditions.checkArgument(shardSize > 0, "The size of the shard should be more than zero");
        }

        @Override
        public void reduce(Iterable<CoocTuple> coocs, Collector<Tuple1<Long>> out) throws Exception {

            List<Long> globalIndicesRow = new LinkedList<>();
            List<Long> globalIndicesCol = new LinkedList<>();
            List<Long> localIndicesRow = new LinkedList<>();
            List<Long> localIndicesCol = new LinkedList<>();
            List<Float> localValues = new LinkedList<>();

            int shardRow = -1, shardCol = -1;
            ArrayList<CoocTuple> tempCoocs = new ArrayList<>();
            for (CoocTuple cooc : coocs) {
                tempCoocs.add(new CoocTuple(
                    cooc.left() / shardNumber,
                    cooc.right() / shardNumber,
                    cooc.value()
                ));

                int currentShardRow = cooc.left() % shardNumber;
                int currentShardCol = cooc.right() % shardNumber;
                if (shardRow == -1) {
                    shardRow = currentShardRow;
                    shardCol = currentShardCol;
                }
                if (shardRow != currentShardRow || shardCol != currentShardCol) {
                    throw new IllegalStateException(String.format(
                        "Inconsistent shard row/col: %d vs %d, %d vs %d",
                        currentShardRow, shardRow, currentShardCol, shardCol
                    ));
                }
            }
            tempCoocs.sort((o1, o2) -> {
                int left = o1.left().compareTo(o2.left());
                if (left == 0) {
                    return o1.right().compareTo(o2.right());
                }
                return left;
            });

            // Populating global indices (just all the global indices that are valid for this shard)
            for (int i = 0; i < shardSize; i++) {
                globalIndicesRow.add((long) shardRow + i * shardNumber);
                globalIndicesCol.add((long) shardCol + i * shardNumber);
            }

            for (CoocTuple cooc : tempCoocs) {
                localIndicesRow.add((long) cooc.left());
                localIndicesCol.add((long) cooc.right());
                localValues.add(cooc.value());
            }

            FileOutputStream fwdStream = getStream(shardRow, shardCol);
            example(globalIndicesRow, globalIndicesCol, localIndicesRow, localIndicesCol, localValues).writeTo(fwdStream);
            fwdStream.close();

            out.collect(new Tuple1<>((long) localValues.size()));
        }

        private Example example(List<Long> globalIndicesRow, List<Long> globalIndicesCol, List<Long> localIndicesRow, List<Long> localIndicesCol, List<Float> localValues) {
            return Example
                .newBuilder()
                .setFeatures(Features
                    .newBuilder()
                    .putFeature("global_row", intFeature(globalIndicesRow))
                    .putFeature("global_col", intFeature(globalIndicesCol))
                    .putFeature("sparse_local_row", intFeature(localIndicesRow))
                    .putFeature("sparse_local_col", intFeature(localIndicesCol))
                    .putFeature("sparse_value", floatFeature(localValues))
                    .build())
                .build();
        }

        private FileOutputStream getStream(int shardRow, int shardCol) throws FileNotFoundException {
            return new FileOutputStream(new File(new File(workPath, "shards"), String.format("shard-%03d-%03d.pb", shardRow, shardCol)));
        }

        private Feature intFeature(Collection<Long> value) {
            return Feature
                .newBuilder()
                .setInt64List(intList(value))
                .build();
        }

        private Feature floatFeature(Collection<Float> value) {
            return Feature
                .newBuilder()
                .setFloatList(floatList(value))
                .build();
        }

        private Int64List intList(Collection<Long> value) {
            return Int64List
                .newBuilder()
                .addAllValue(value)
                .build();
        }

        private FloatList floatList(Collection<Float> value) {
            return FloatList
                .newBuilder()
                .addAllValue(value)
                .build();
        }
    }

    private static class HashPartitioner implements KeySelector<CoocTuple, String> {

        private int numShards;

        public HashPartitioner(int numShards) {
            this.numShards = numShards;
        }

        @Override
        public String getKey(CoocTuple value) throws Exception {
            return String.format("%d-%d", value.f0 % numShards, value.f1 % numShards);
        }
    }

    private void tweets(String workPath) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final Configuration parameters = new Configuration();
        parameters.setInteger(WINDOW_SIZE, windowSize);
        parameters.setInteger(SHARD_SIZE, shardSize);
        parameters.setString(WORK_PATH, workPath);

        //Creating a directory for shards if not exists
        new File(workPath, "shards").mkdir();

        //Reading vocabulary with frequences assuming the input is sorted
        TypeHint freqTuple = new TypeHint<Tuple2<String, Integer>>(){};
        DataSet<Tuple3<Integer, String, Integer>> vocabulary =
            getInput(env, new Path(workPath, "dict.tsv.gz"))
                .map((MapFunction<String, Tuple2<String, Integer>>) value -> {
                    String[] row = value.split("\t");
                    checkState(row.length == 2);
                    return new Tuple2<>(row[0], Integer.valueOf(row[1]));
                })
                .returns(freqTuple)
                .first(vocabularySize)
                .mapPartition(new Zipper()).setParallelism(1);

        final long realVocabularySize = vocabulary.count();
        final int numShards = (int) (realVocabularySize / shardSize);

        parameters.setInteger(SHARD_NUMBER, numShards);

        //Reading tweet stream and producing co-occurrences
        DataSet<CoocTuple> coocs =
            getInput(env, new Path(workPath, "tweet_text"))
                .flatMap(new CooccurrenceExtractor())
                    .withBroadcastSet(vocabulary, "dictionary").withParameters(parameters)
                .groupBy(0, 1).sum(2);

        //Writing co-occurrences to disk
        coocs
            .groupBy(new HashPartitioner(numShards))
            .reduceGroup(new CoocWriter()).withParameters(parameters)
            .sum(0)
            .output(new TextOutputFormat<>(new Path(workPath, "coocs.log"))).setParallelism(1);

        //Dumping row and column vocabularies
        Path shardsPath = new Path(workPath, "shards");
        DataSet<String> rawVocabulary = vocabulary.map(value -> value.f1).returns(String.class);
        rawVocabulary
            .output(new TextOutputFormat<>(new Path(shardsPath, "row_vocab.txt"))).setParallelism(1);
        rawVocabulary
            .output(new TextOutputFormat<>(new Path(shardsPath, "col_vocab.txt"))).setParallelism(1);

        //Producing marginal sums
        DataSet<MarginalTuple> marginals =
            produceMarginals(coocs, realVocabularySize);

        //Dumping row and column marginals
        marginals
            .output(new RobustTsvOutputFormat<>(new Path(workPath, "shard-marginals.tsv")));

        DataSet<Float> rawMarginals = marginals.map(value -> value.f1).returns(Float.class);
        rawMarginals
            .output(new TextOutputFormat<>(new Path(shardsPath, "row_sums.txt")));
        rawMarginals
            .output(new TextOutputFormat<>(new Path(shardsPath, "col_sums.txt")));

        //Writing full dictionary
        vocabulary
            .output(new RobustTsvOutputFormat<>(new Path(workPath, "shard-dict.tsv"))).setParallelism(1);

        env.execute();
    }

    private DataSet<MarginalTuple> produceMarginals(DataSet<CoocTuple> coocs, long realVocabularySize) {
        return coocs
            .flatMap((FlatMapFunction<CoocTuple, MarginalTuple>) (value, out) -> {
                out.collect(new MarginalTuple(value.left(), value.value()));
                if (!value.left().equals(value.right())) {
                    out.collect(new MarginalTuple(value.right(), value.value()));
                }
            })
            .returns(MarginalTuple.class)
            .groupBy(0).sum(1)
            .sortPartition(0, Order.ASCENDING).setParallelism(1)
            .reduceGroup((GroupReduceFunction<MarginalTuple, MarginalTuple>) (values, out) -> {
                int lastInt = 0;
                for (MarginalTuple value : values) {
                    lastInt++;
                    while (value.f0 > ++lastInt) {
                        out.collect(MarginalTuple.zero(lastInt));
                    }
                    out.collect(value);
                }
                while (realVocabularySize > ++lastInt) {
                    out.collect(MarginalTuple.zero(lastInt));
                }
            })
            .returns(MarginalTuple.class);
    }

    public static final class CoocTuple extends Tuple3<Integer, Integer, Float> {

        public CoocTuple() { super(); }
        public CoocTuple(Integer leftId, Integer rightId, Float value) {
            super(leftId, rightId, value);
        }

        public Integer left() {
            return this.f0;
        }

        public Integer right() {
            return this.f1;
        }

        public Float value() {
            return this.f2;
        }
    }

    public static final class MarginalTuple extends Tuple2<Integer, Float> {

        public MarginalTuple() { super(); }
        public MarginalTuple(Integer id, Float value) {
            super(id, value);
        }

        public static MarginalTuple zero(int id) {
            return new MarginalTuple(id, 0.0f);
        }
    }

    public static final class FreqTuple extends Tuple2<String, Integer> {
        public FreqTuple(String value0, Integer value1) {
            super(value0, value1);
        }
    }

    private static CommandLine.Parser provideParameterList() {
        return CommandLine.parser()
            .withOption("w", WORK_PATH,
                "work directory", "DIRECTORY",
                CommandLine.Type.STRING, true, false, true)
                .withOption(null, WINDOW_SIZE,
                        "window size", "SIZE",
                        CommandLine.Type.INTEGER, true, false, false)
                .withOption(null, VOCABULARY_SIZE,
                        "vocabulary size", "SIZE",
                        CommandLine.Type.INTEGER, true, false, false)
                .withOption(null, SHARD_SIZE,
                        "shard size size", "SIZE",
                        CommandLine.Type.INTEGER, true, false, false);
    }

    public void setVocabularySize(@Nullable Integer vocabularySize) {
        if (vocabularySize != null) {
            this.vocabularySize = vocabularySize;
        }
    }

    public void setWindowSize(@Nullable Integer windowSize) {
        if (windowSize != null) {
            this.windowSize = windowSize;
        }
    }

    public void setShardSize(@Nullable Integer shardSize) {
        if (shardSize != null) {
            this.shardSize = shardSize;
        }
    }

    public static void main(String[] args) throws Exception {
        PopulateCooccurrenceMatrix extractor = new PopulateCooccurrenceMatrix();

        try {
            // Parse command line
            final CommandLine cmd = provideParameterList().parse(args);

            extractor.setVocabularySize(cmd.getOptionValue(VOCABULARY_SIZE, Integer.class));
            extractor.setWindowSize(cmd.getOptionValue(WINDOW_SIZE, Integer.class));
            extractor.setShardSize(cmd.getOptionValue(SHARD_SIZE, Integer.class));

            final String workPath = cmd.getOptionValue(WORK_PATH, String.class);

            //noinspection ConstantConditions
            extractor.tweets(workPath);
        } catch (final Throwable ex) {
            // Handle exception
            CommandLine.fail(ex);
        }
    }
}
