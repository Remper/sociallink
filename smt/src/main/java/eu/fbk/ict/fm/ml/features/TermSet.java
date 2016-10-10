package eu.fbk.ict.fm.ml.features;

import org.fbk.cit.hlt.core.analysis.stemmer.Stemmer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * A modified version of TermSet from jcore
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class TermSet {
  private static final Logger logger = LoggerFactory.getLogger(TermSet.class);
  private static final Pattern tabPattern = Pattern.compile("\t");

  public final static boolean DEFAULT_LOWERCASE = true;
  public final static int DEFAULT_COLUMN = 0;

  protected Set<String> set;
  private int maxSize;
  private int column;
  private boolean lowercase;
  private Stemmer stemmer;

  public TermSet() {
    this(Integer.MAX_VALUE);
  }

  public TermSet(int maxSize) {
    this.maxSize = maxSize;
    lowercase = DEFAULT_LOWERCASE;
    column = DEFAULT_COLUMN;
    set = new HashSet<>();
  }

  public Stemmer getStemmer() {
    return stemmer;
  }

  public void setStemmer(Stemmer stemmer) {
    this.stemmer = stemmer;
  }

  public boolean getLowercase() {
    return lowercase;
  }

  public void setLowercase(boolean lowercase) {
    this.lowercase = lowercase;
  }

  public int getColumn() {
    return column;
  }

  public void setColumn(int column) {
    this.column = column;
  }

  public int getMaxSize() {
    return maxSize;
  }

  public void setMaxSize(int maxSize) {
    this.maxSize = maxSize;
  }

  public boolean contains(String w) {
    return set.contains(filter(w));
  }

  private String filter(String input) {
    if (lowercase) {
      return input.trim().toLowerCase();
    }
    return input;
  }

  private String stem(String input) {
    if (stemmer != null) {
      return stemmer.stem(input);
    }
    return input;
  }

  public int size() {
    return set.size();
  }

  public void read(Reader in, Stemmer stemmer) throws IOException {
    this.stemmer = stemmer;
    read(in);
  }

  public void read(Reader in) throws IOException {
    logger.debug("Reading term set...");

    LineNumberReader lnr = new LineNumberReader(in);
    String line;
    String[] s;
    while ((line = lnr.readLine()) != null && set.size() < maxSize) {
      line = line.trim();
      if (line.startsWith("#") || line.length() == 0) {
        continue;
      }

      s = tabPattern.split(line);
      if (s.length <= column) {
        continue;
      }
      set.add(stem(filter(s[column])));
    }
    lnr.close();
    logger.debug(set.size() + " terms read");
  }

  public void write(Writer writer) throws IOException {
    logger.info("writing term set...");
    PrintWriter pw = new PrintWriter(writer);
    // iterates over the training set
    for (String s : set) {
      pw.println(s);
      pw.flush();
    }
    pw.close();

  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (Object s : set) {
      sb.append(s);
      sb.append("\n");
    }

    return sb.toString();
  }
}