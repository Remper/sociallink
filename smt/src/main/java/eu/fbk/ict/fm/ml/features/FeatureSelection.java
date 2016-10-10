package eu.fbk.ict.fm.ml.features;

import eu.fbk.ict.fm.ml.features.methods.Aprosio;
import eu.fbk.ict.fm.ml.features.methods.FeatureSelectionMethod;
import org.fbk.cit.hlt.core.util.HashMultiSet;
import org.fbk.cit.hlt.core.util.MultiSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.DecimalFormat;
import java.util.*;

/**
 * Extracts features and chooses the best ones
 *
 * @author Claudio Giuliano (giuliano@fbk.eu)
 */
public class FeatureSelection implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(FeatureSelection.class);
  private static final DecimalFormat df = new DecimalFormat("###,###,###,###");

  public static final int DEFAULT_MIN_FEATURE_FREQ = 5;
  public static final int DEFAULT_FEATURE_SET_SIZE = 1000;
  public static final boolean DEFAULT_EXTRACT_VECTORS = false;
  public static final boolean DEFAULT_HEADER = false;

  private Set<String> categorySet = new HashSet<>();
  private MultiSet<String> featureMultiSet = new HashMultiSet<>();
  private List<String> categoryList = new ArrayList<>();
  private List<Set<String>> instanceList = new ArrayList<>();
  private List<String> textList = new ArrayList<>();
  private FeatureExtraction featureExtraction = new FeatureExtraction();
  private FeatureSelectionMethod selectionMethod = new Aprosio();

  private int instanceSize = Integer.MAX_VALUE;
  private int minFeatureFreq = DEFAULT_MIN_FEATURE_FREQ;
  private int featureSetSize = DEFAULT_FEATURE_SET_SIZE;
  private boolean extractVectors = DEFAULT_EXTRACT_VECTORS;
  private boolean header = DEFAULT_HEADER;
  private String outputRoot;

  private PrintWriter logWriter = null;

  public FeatureSelection(String outputRoot) {
    this.outputRoot = outputRoot;
  }

  private String createOutputFileName(String suffix) {
    StringBuilder sb = new StringBuilder();
    sb.append(outputRoot);
    sb.append("-fs-");
    sb.append(selectionMethod.getClass().getSimpleName().toLowerCase());
    if (featureExtraction.isSparse()) {
      sb.append("-sparse");
    }

    sb.append("-n-").append(featureExtraction.getMaxNGramsLength());
    if (featureExtraction.getStemmer() != null) {
      sb.append("-stem");
    }
    sb.append("-min-freq-").append(getMinFeatureFreq());
    sb.append("-feat-").append(getFeatureSetSize());
    sb.append("-inst-");
    if (getInstanceSize() == Integer.MAX_VALUE) {
      sb.append("max");
    } else {
      sb.append(getInstanceSize());
    }

    sb.append(suffix);
    return sb.toString();
  }

  public PrintWriter getLogger() throws IOException {
    if (logWriter == null) {
      String logFileName = createOutputFileName(".log.tsv");
      logWriter = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(logFileName), "UTF-8")));
    }

    return logWriter;
  }

  public void run() throws IOException {
    logger.info("selecting features (" + new Date() + ")...");
    long begin = System.currentTimeMillis();

    FeatureList rankedList = extract();
    List<FeatureList.Entry> list = rankedList.toList();
    writeFeatureList(list);
    if (extractVectors) {
      writePseudoVectors(rankedList);
      writeText();
      writeVectors(rankedList);
      writeIndex(rankedList);
      writeConfig();
    }

    long end = System.currentTimeMillis();
    logger.info(df.format(list.size()) + " feature selected in " + df.format(end - begin) + " ms (" + new Date() + ")");
  }

  public void run(File inputFile) throws IOException {
    logger.info("Reading input file...");
    long begin = System.currentTimeMillis();
    read(inputFile);
    long end = System.currentTimeMillis();
    logger.info("Done in..." + df.format(end - begin) + " ms");
    run();
  }

  private void writeConfig() throws IOException {
    String configurationFileName = createOutputFileName(".conf");
    logger.info("writing configuration in " + configurationFileName + " (" + new Date() + ")...");
    PrintWriter configurationWriter = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(configurationFileName), "UTF-8")));

    long begin = System.currentTimeMillis();
    configurationWriter.println("#model built " + new Date());
    configurationWriter.println("output= " + outputRoot);
    configurationWriter.println("frequency= " + minFeatureFreq);

    if (featureExtraction.getStemmer() != null) {
      configurationWriter.println("stemmer=" + featureExtraction.getLanguage());
    }
    configurationWriter.println("n=" + featureExtraction.getMaxNGramsLength());
    configurationWriter.println("sparse=" + featureExtraction.isSparse());
    configurationWriter.println("white=" + (featureExtraction.getWhiteList() != null ? featureExtraction.getWhiteList().size() : 0));
    configurationWriter.println("black=" + (featureExtraction.getBlackList() != null ? featureExtraction.getBlackList().size() : 0));

    configurationWriter.println("metric=" + selectionMethod.getClass().getSimpleName());
    configurationWriter.println("instances=" + getInstanceSize());
    configurationWriter.println("features=" + getFeatureSetSize());
    configurationWriter.close();
    long end = System.currentTimeMillis();
    logger.info(df.format(instanceList.size()) + " configuration wrote in " + df.format(end - begin) + " ms (" + new Date() + ")");
  }

  private void writeIndex(FeatureList rankedList) throws IOException {
    String indexFileName = createOutputFileName(".feat");
    logger.info("writing index in " + indexFileName + " (" + new Date() + ")...");
    Writer indexWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(indexFileName), "UTF-8"));

    long begin = System.currentTimeMillis();
    rankedList.writeIndex(indexWriter);
    indexWriter.close();
    long end = System.currentTimeMillis();
    logger.info(df.format(instanceList.size()) + " index wrote in " + df.format(end - begin) + " ms (" + new Date() + ")");
  }

  private void writeText() throws IOException {

    String vectorsFileName = createOutputFileName(".text");
    logger.info("writing text in " + vectorsFileName + " (" + new Date() + ")...");
    PrintWriter vectorsWriter = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(vectorsFileName), "UTF-8")));
    long begin = System.currentTimeMillis();

    for (int i = 0; i < textList.size(); i++) {
      String text = textList.get(i);
      String category = categoryList.get(i);
      vectorsWriter.println(category + "\t" + text);
    }
    vectorsWriter.close();
    long end = System.currentTimeMillis();
    logger.info(df.format(textList.size()) + " text wrote in " + df.format(end - begin) + " ms (" + new Date() + ")");
  }

  private void writePseudoVectors(FeatureList rankedList) throws IOException {

    String vectorsFileName = createOutputFileName(".txt");
    logger.info("writing pseudo vectors in " + vectorsFileName + " (" + new Date() + ")...");
    PrintWriter vectorsWriter = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(vectorsFileName), "UTF-8")));
    long begin = System.currentTimeMillis();

    for (int i = 0; i < instanceList.size(); i++) {
      Set<String> features = instanceList.get(i);
      String category = categoryList.get(i);
      String vector = createPseudoVector(features, rankedList);
      vectorsWriter.println(category + vector);
    }
    vectorsWriter.close();
    long end = System.currentTimeMillis();
    logger.info(df.format(instanceList.size()) + " vectors wrote in " + df.format(end - begin) + " ms (" + new Date() + ")");
  }

  private void writeVectors(FeatureList rankedList) throws IOException {
    String vectorsFileName = createOutputFileName(".svm");
    logger.info("writing vectors in " + vectorsFileName + " (" + new Date() + ")...");
    PrintWriter vectorsWriter = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(vectorsFileName), "UTF-8")));
    long begin = System.currentTimeMillis();

    for (int i = 0; i < instanceList.size(); i++) {
      Set<String> features = instanceList.get(i);
      String category = categoryList.get(i);
      String vector = createVector(features, rankedList);
      vectorsWriter.println(category + vector);
    }
    vectorsWriter.close();
    long end = System.currentTimeMillis();
    logger.info(df.format(instanceList.size()) + " vectors wrote in " + df.format(end - begin) + " ms (" + new Date() + ")");
  }


  private String pseudoVectorToString(SortedMap<Integer, String> vector) {
    StringBuilder sb = new StringBuilder();
    Iterator<Integer> it = vector.keySet().iterator();
    for (int i = 0; it.hasNext(); i++) {
      Integer index = it.next();
      String feature = vector.get(index);
      if (feature != null) {
        sb.append(" ");
        sb.append(index);
        sb.append(":");
        sb.append(feature);
        logger.trace(i + "\t" + index + ":" + feature);
      }
    }
    logger.trace(sb.toString());
    return sb.toString();
  }

  private String vectorToString(SortedMap<Integer, Double> vector) {
    StringBuilder sb = new StringBuilder();
    Iterator<Integer> it = vector.keySet().iterator();
    for (int i = 0; it.hasNext(); i++) {
      Integer index = it.next();
      Double weight = vector.get(index);
      if (weight > 0) {
        sb.append(" ");
        sb.append(index);
        sb.append(":");
        sb.append(weight);
        logger.trace(i + "\t" + index + ":" + weight);
      }
    }
    logger.trace(sb.toString());
    return sb.toString();
  }

  private String createPseudoVector(Set<String> features, FeatureList rankedList) {
    Iterator<String> it = features.iterator();
    SortedMap<Integer, String> vector = new TreeMap<>();
    logger.trace(features.toString());
    for (int i = 0; it.hasNext(); i++) {
      String feature = it.next();
      Integer index = rankedList.getIndex(feature);
      logger.trace(i + "\t" + feature + "\t" + index + ":" + feature);
      if (feature != null && index != null) {
        vector.put(index, feature);
      }

    }
    return pseudoVectorToString(vector);
  }

  private String createVector(Set<String> features, FeatureList rankedList) {
    Iterator<String> it = features.iterator();
    SortedMap<Integer, Double> vector = new TreeMap<>();
    logger.trace(features.toString());
    for (int i = 0; it.hasNext(); i++) {
      String feature = it.next();
      Double weight = rankedList.getWeight(feature);
      Integer index = rankedList.getIndex(feature);
      logger.trace(i + "\t" + feature + "\t" + index + ":" + weight);
      if (weight != null && index != null) {
        vector.put(index, weight);
      }

    }
    return vectorToString(vector);
  }

  private void writeFeatureList(List<FeatureList.Entry> list) throws IOException {
    String featureListName = createOutputFileName(".rank.tsv");
    long begin = System.currentTimeMillis();
    logger.info("writing feature list " + featureListName + " (" + new Date() + ")...");
    PrintWriter pw = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(featureListName), "UTF-8")));
    int i = 0;
    for (FeatureList.Entry entry : list) {
      pw.println(entry.toString());
      if (i >= featureSetSize) {
        break;
      }
      i++;
    }
    pw.flush();
    pw.close();
    long end = System.currentTimeMillis();
    logger.info(df.format(i) + " features wrote in " + df.format(end - begin) + " ms (" + new Date() + ")");
  }

  private FeatureList extract() throws IOException {
    long begin = System.currentTimeMillis();

    FeatureList featureList = new FeatureList();
    Iterator<String> it = featureMultiSet.iterator();
    logger.info("selecting from " + df.format(featureMultiSet.size()) + " features...");
    for (int i = 1; it.hasNext(); i++) {
      String feature = it.next();
      int freq = featureMultiSet.getFrequency(feature);
      if (freq > minFeatureFreq) {
        // max score could be multi-threading

        double score = maxScore(feature);
        featureList.put(feature, score);
      }
      if ((i % 10000) == 0) {
        long time = System.currentTimeMillis() - begin;
        logger.info(df.format(i) + "\t" + df.format(featureList.size()) + "\t" + df.format(featureMultiSet.size()) + "\t" + df.format(time) + "\t" + new Date());
        begin = System.currentTimeMillis();
      }
    }

    logger.info(df.format(featureList.size()) + " features (frequency > " + df.format(minFeatureFreq) + ") weighted");
    return featureList;
  }

  /**
   * The feature is ranked using its max category score
   *
   * @param feature Feature to rank
   * @return maximum score
   */
  protected double maxScore(String feature) {
    double maxScore = 0;
    for (String category : categorySet) {
      double score = categoryScore(feature, category);
      if (score > maxScore) {
        maxScore = score;
      }
    }
    return maxScore;
  }

  // finds the max category score for the specified feature
  protected double categoryScore(String feature, String category) {
    int A = 0, B = 0, C = 0, D = 0;
    //iterates over the instances
    for (int i = 0; i < instanceList.size(); i++) {
      String y = categoryList.get(i);
      Set<String> features = instanceList.get(i);

      if (features.contains(feature)) {
        // the document contains the feature
        if (y.equals(category)) {
          // the document has label category
          A++;
        } else {
          // the document doesn't have label category
          B++;
        }
      } else {
        // the document doesn't contain the feature
        if (y.equals(category)) {
          // the document has label category
          C++;
        } else {
          // the document doesn't have label category
          D++;
        }
      }
    }

    // calculates the score using the specified metric
    double score = selectionMethod.categoryScore(feature, category, A, B, C, D, instanceList.size());
    try {
      getLogger().println(feature + "\t" + category + "\t" + A + "\t" + B + "\t" + C + "\t" + D + "\t" + instanceList.size() + "\t" + score);
    } catch (IOException e) {
      logger.warn("Exception", e);
    }

    return score;
  }

  public void addSample(String label, String text) {
    Set<String> features = featureExtraction.extract(text);
    categorySet.add(label);
    featureMultiSet.addAll(features);
    categoryList.add(label);
    instanceList.add(features);
    textList.add(text);
  }

  private void read(File inputFile) throws IOException {
    logger.info("reading instances from " + inputFile + " (" + new Date() + ")...");
    long begin = System.currentTimeMillis();
    LineNumberReader lnr = new LineNumberReader(new FileReader(inputFile));

    String line;
    if (header && (line = lnr.readLine()) != null) {
      logger.info("reading header: " + line);
    }
    int i = 1;
    for (; (line = lnr.readLine()) != null; ) {
      String[] s = line.split("\t");

      if (s.length == 2) {
        // text 0 label 1
        logger.trace(s[0] + "\t" + s[1]);
        addSample(s[1], s[0]);
      } else if (s.length == 4) {
        // text 2 label 1
        logger.trace(s[1] + "\t" + s[2]);
        if (s[1].length() > 0) {
          addSample(s[2], s[1]);
        }
      }
      if ((i % 10000) == 0) {
        logger.info(df.format(i) + " lines read (" + new Date() + ")");
      }

      if (instanceSize < i) {
        break;
      }
      i++;
    }
    long end = System.currentTimeMillis();
    logger.info(df.format(instanceList.size()) + " instances read in " + df.format(end - begin) + " ms (" + new Date() + ")");
    lnr.close();
  }

  public boolean isHeader() {
    return header;
  }

  public void setHeader(boolean header) {
    this.header = header;
  }

  public boolean isExtractVectors() {
    return extractVectors;
  }

  public void setExtractVectors(boolean extractVectors) {
    this.extractVectors = extractVectors;
  }

  public int getMinFeatureFreq() {
    return minFeatureFreq;
  }

  public void setMinFeatureFreq(int minFeatureFreq) {
    this.minFeatureFreq = minFeatureFreq;
  }

  public FeatureExtraction getFeatureExtraction() {
    return featureExtraction;
  }

  public void setFeatureExtraction(FeatureExtraction featureExtraction) {
    this.featureExtraction = featureExtraction;
  }

  public int getFeatureSetSize() {
    return featureSetSize;
  }

  public void setFeatureSetSize(int featureSetSize) {
    this.featureSetSize = featureSetSize;
  }

  public int getInstanceSize() {
    return instanceSize;
  }

  public void setInstanceSize(int instanceSize) {
    this.instanceSize = instanceSize;
  }

  public FeatureSelectionMethod getSelectionMethod() {
    return selectionMethod;
  }

  public void setSelectionMethod(FeatureSelectionMethod selectionMethod) {
    this.selectionMethod = selectionMethod;
  }

  @Override
  public void close() throws IOException {
    logWriter.close();
    logWriter = null;
  }
}