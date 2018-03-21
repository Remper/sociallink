package eu.fbk.fm.ml.features;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Remaps dataset with features produced by simple tokenizers to our Stemmer
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class Remapper {
  public final static String INTERCEPT = "_intercept";

  Iterable<CSVRecord> records;
  FeatureExtraction extraction;

  public Remapper(FeatureExtraction extraction, Iterable<CSVRecord> records) {
    this.records = records;
    this.extraction = extraction;
    extraction.setMaxNGramsLength(3);
  }

  public void saveTo(File file) throws IOException {
    CSVPrinter printer = CSVFormat.DEFAULT.withHeader("term", "weight").print(new BufferedWriter(new FileWriter(file)));
    for (CSVRecord record : records) {
      int maxTokenSize = 0;
      Set<String> tokenSet = new HashSet<>();
      String rawToken = record.get("term");
      if (rawToken.equals(INTERCEPT)) {
        tokenSet.add(INTERCEPT);
      } else {
        for (String token : extraction.extract(rawToken)) {
          int curTokenSize = token.split("_").length;
          if (maxTokenSize < curTokenSize) {
            maxTokenSize = curTokenSize;
            tokenSet.clear();
          }
          if (curTokenSize == maxTokenSize) {
            tokenSet.add(token);
          }
        }
      }
      for (String token : tokenSet) {
        printer.printRecord(token, record.get("weight"));
      }
    }
    printer.close();
  }
}
