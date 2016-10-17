package eu.fbk.ict.fm.data.ngrams;

import eu.fbk.ict.fm.data.dataset.FeatureMapping;
import java.util.List;

/**
 * Commong methods for different feature mapping's implementations
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public interface FeatureMappingInterface {
    FeatureMapping.Feature lookup(String ngram);
    List<FeatureMapping.Feature> lookup(List<String> ngrams);
}
