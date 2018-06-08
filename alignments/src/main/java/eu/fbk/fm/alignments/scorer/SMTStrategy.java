package eu.fbk.fm.alignments.scorer;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import eu.fbk.fm.alignments.DBpediaResource;
import eu.fbk.fm.alignments.Evaluate;
import eu.fbk.fm.alignments.scorer.embeddings.EntityDirectEmbeddings;
import eu.fbk.fm.alignments.scorer.embeddings.SocialGraphEmbeddings;
import eu.fbk.fm.alignments.scorer.text.LSAVectorProvider;
import eu.fbk.fm.alignments.scorer.text.VectorProvider;
import eu.fbk.fm.alignments.twitter.TwitterDeserializer;
import eu.fbk.fm.vectorize.preprocessing.text.TextExtractor;
import eu.fbk.utils.lsa.LSM;
import eu.fbk.utils.math.DenseVector;
import eu.fbk.utils.math.Vector;
import twitter4j.User;

import javax.sql.DataSource;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.LinkedList;
import java.util.function.BiFunction;
import java.util.stream.StreamSupport;

public class SMTStrategy extends PAI18Strategy {

    public SMTStrategy(DataSource source, String lsaPath) throws Exception {
        super(initProviders(source, lsaPath));
    }

    private static LinkedList<FeatureVectorProvider> initProviders(DataSource source, String lsaPath) throws IOException, URISyntaxException {
        LSM lsm = new LSM(lsaPath+"/X", 100, true);
        LSAVectorProvider provider = new LSAVectorProvider(lsm);
        return new LinkedList<FeatureVectorProvider>(){{
            add(new ProfileFeatureProvider(lsm));
            add(new TextProvider("dbpedia", provider, DBPEDIA_TEXT_EXTRACTOR));
            add(new TextProvider("tweets", provider, USER_TEXT_EXTRACTOR));
            add(new EntityDirectEmbeddings(source, "kb200_rdf2vec"));
            add(new SocialGraphEmbeddings(source, "sg300"));
        }};
    }

    public static BiFunction<User, DBpediaResource, String> DBPEDIA_TEXT_EXTRACTOR =
        (user, resource) -> TextScorer
            .getResourceTexts(resource)
            .stream()
            .reduce((text1, text2) -> text1 + " " + text2)
            .orElse("");

    public static BiFunction<User, DBpediaResource, String> USER_TEXT_EXTRACTOR =
            (user, resource) -> {
                if (!(user instanceof UserData)) {
                    return "";
                }
                Gson gson = TwitterDeserializer.getDefault().getBuilder().create();
                TextExtractor extractor = new TextExtractor(false);
                JsonElement array = ((UserData) user)
                    .get(Evaluate.StatusesProvider.class)
                    .orElse(new JsonArray());
                if (!array.isJsonArray()) {
                    return "";
                }
                return StreamSupport
                    .stream(((JsonArray) array).spliterator(), false)
                    .map(extractor::map)
                    .reduce((text1, text2) -> text1 + " " + text2)
                    .orElse("");
            };

    public static class TextProvider implements FeatureVectorProvider {
        final String suffix;
        final VectorProvider provider;
        final BiFunction<User, DBpediaResource, String> textExtractor;

        public TextProvider(String suffix, VectorProvider provider, BiFunction<User, DBpediaResource, String> textExtractor) {
            this.suffix = suffix;
            this.provider = provider;
            this.textExtractor = textExtractor;
        }

        @Override
        public double[] getFeatures(User user, DBpediaResource resource) {
            Vector vector = provider.toVector(textExtractor.apply(user, resource));
            double[] result = new double[vector.size()];
            for (int i = 0; i < vector.size(); i++) {
                result[i] = vector.get(i);
            }
            return result;
        }

        @Override
        public String getSubspaceId() {
            return "text_"+provider.toString()+"_"+suffix;
        }
    }
}
