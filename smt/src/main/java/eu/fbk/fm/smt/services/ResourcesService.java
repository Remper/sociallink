package eu.fbk.fm.smt.services;

import eu.fbk.utils.data.DatasetRepository;
import eu.fbk.utils.data.dataset.Binary;
import eu.fbk.utils.data.dataset.CSVDataset;
import eu.fbk.utils.data.dataset.Dataset;
import eu.fbk.utils.data.dataset.bow.FeatureMapping;

import javax.inject.Singleton;

/**
 * Provides resources for user profiling
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
@Singleton
public class ResourcesService {
  public static final String NAMES_WIKIDATA = "gender.names.wikidata.clean";
  public static final String NAMES_FACEBOOK = "gender.names.facebook";
  public static final String AGE_LEXICA = "age.lexica";
  public static final String LOCATION_COUNTRIES = "location.countries";
  public static final String LOCATION_COUNTRIES_SIMPLE = "location.countries.simple";
  public static final String NGRAMS = "ngrams";
  public static final String DBPEDIA_STATISTICS = "dbpedia.statistics";
  public static final String ALIGNMENTS_HOMEPAGE_FILTERED = "alignments.homepage.filtered";
  public static final String ALIGNMENTS_HOMEPAGE_EXTRACTED = "alignments.homepage.extracted";

  public FeatureMapping provideNGrams(DatasetRepository repository) throws Exception {
    Dataset dataset = repository.load(NGRAMS);
    if (!(dataset instanceof FeatureMapping)) {
      throw new Exception("The instantiated dataset is of the wrong type");
    }
    return (FeatureMapping) dataset;
  }

  public CSVDataset provideExtractedHomepageAlignments(DatasetRepository repository) throws Exception {
    return provideCSVAndCheckType(repository, ALIGNMENTS_HOMEPAGE_EXTRACTED);
  }

  public CSVDataset provideFilteredHomepageAlignments(DatasetRepository repository) throws Exception {
    return provideCSVAndCheckType(repository, ALIGNMENTS_HOMEPAGE_FILTERED);
  }

  public CSVDataset provideDbpediaStatistics(DatasetRepository repository) throws Exception {
    return provideCSVAndCheckType(repository, DBPEDIA_STATISTICS);
  }

  public CSVDataset provideLexica(DatasetRepository repository) throws Exception {
    return provideCSVAndCheckType(repository, AGE_LEXICA);
  }

  public CSVDataset provideNames(DatasetRepository repository) throws Exception {
    return provideCSVAndCheckType(repository, NAMES_WIKIDATA);
  }

  public CSVDataset provideFacebookNames(DatasetRepository repository) throws Exception {
    return provideCSVAndCheckType(repository, NAMES_FACEBOOK);
  }

  public Binary provideCountries(DatasetRepository repository) throws Exception {
    return provideBinaryAndCheckType(repository, LOCATION_COUNTRIES);
  }

  public Binary provideSimpleCountries(DatasetRepository repository) throws Exception {
    return provideBinaryAndCheckType(repository, LOCATION_COUNTRIES_SIMPLE);
  }

  private Binary provideBinaryAndCheckType(DatasetRepository repository, String name) throws Exception {
    Dataset dataset = repository.load(name);
    if (!(dataset instanceof Binary)) {
      throw new Exception("The instantiated dataset is of the wrong type");
    }
    return (Binary) dataset;
  }

  private CSVDataset provideCSVAndCheckType(DatasetRepository repository, String name) throws Exception {
    Dataset dataset = repository.load(name);
    if (!(dataset instanceof CSVDataset)) {
      throw new Exception("The instantiated dataset is of the wrong type");
    }
    return (CSVDataset) dataset;
  }
}
