package eu.fbk.fm.alignments.scorer;

import eu.fbk.fm.alignments.kb.KBResource;
import eu.fbk.fm.alignments.scorer.embeddings.SocialGraphEmbeddings;
import eu.fbk.fm.alignments.scorer.text.LSAVectorProvider;
import eu.fbk.fm.alignments.scorer.text.VectorProvider;
import eu.fbk.utils.lsa.LSM;
import eu.fbk.utils.math.Vector;
import twitter4j.User;

import javax.sql.DataSource;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.LinkedList;
import java.util.function.BiFunction;

import static eu.fbk.fm.alignments.scorer.TextScorer.DBPEDIA_TEXT_EXTRACTOR;

public class SMTStrategy extends PAI18Strategy {

    public SMTStrategy(DataSource source, String lsaPath) throws Exception {
        super(initProviders(source, lsaPath));
    }

    public SMTStrategy(DataSource source, VectorProvider provider) throws Exception {
        super(initProviders(source, provider));
    }

    private static LinkedList<FeatureVectorProvider> initProviders(DataSource source, String lsaPath) throws IOException, URISyntaxException {
        LSM lsm = new LSM(lsaPath+"/X", 100, true);
        LSAVectorProvider provider = new LSAVectorProvider(lsm);
        return initProviders(source, provider);
    }

    private static LinkedList<FeatureVectorProvider> initProviders(DataSource source, VectorProvider provider) throws IOException, URISyntaxException {
        return new LinkedList<FeatureVectorProvider>(){{
            add(new ProfileFeatureProvider(provider));
            add(new TextProvider("dbpedia", provider, DBPEDIA_TEXT_EXTRACTOR));
            add(new TextProvider("tweets", provider, (user, resource) -> TextScorer.getUserDataText(user)));
            add(new SocialGraphEmbeddings(source, "sg300"));
        }};
    }

    public static class TextProvider implements FeatureVectorProvider {
        final String suffix;
        final VectorProvider provider;
        final BiFunction<User, KBResource, String> textExtractor;

        public TextProvider(String suffix, VectorProvider provider, BiFunction<User, KBResource, String> textExtractor) {
            this.suffix = suffix;
            this.provider = provider;
            this.textExtractor = textExtractor;
        }

        @Override
        public double[] getFeatures(User user, KBResource resource) {
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
