package eu.fbk.ict.fm.data.dataset;

import eu.fbk.ict.fm.data.DatasetMetaInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.LineNumberReader;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Map;

/**
 * Dataset that is just sentence with an integer label
 * Format is:
 * LABEL SENTENCE
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class LabeledSentences extends Dataset implements Closeable {
  final static Logger logger = LoggerFactory.getLogger(LabeledSentences.class);

  LineNumberReader reader;

  public LabeledSentences(DatasetMetaInfo info) throws URISyntaxException, IOException {
    super(info);
    reader = getReader();
  }

  @Override
  public void parse() {
  }

  public ArrayList<Sentence> readAll() {
    //Parse the input file
    ArrayList<Sentence> sentences = new ArrayList<>();
    try (LineNumberReader reader = getReader()) {
      String line;
      while ((line = reader.readLine()) != null) {
        //Sanitizing the the word
        Sentence sentence = Sentence.fromString(line);
        if (sentence == null) {
          continue;
        }
        sentences.add(sentence);
      }

      logger.info("Parsed " + sentences.size() + " sentences");
    } catch (IOException e) {
      logger.error("Can't parse the input file", e);
    }
    return sentences;
  }

  public void reopen() {
    try {
      close();
      reader = getReader();
    } catch (IOException e) {
      logger.error(e.getMessage(), e);
    }
  }

  public Sentence readNext() {
    try {
      String line = reader.readLine();
      return line == null ? null : Sentence.fromString(line);
    } catch (IOException e) {
      logger.error("Can't read from the target file", e);
    }
    return null;
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  public static class Sentence {
    public String label;
    public String sentence;

    public Sentence(String label, String sentence) {
      this.label = label;
      this.sentence = sentence;
    }

    public static Sentence fromString(String line) {
      String[] elements = line.split("\t");
      if (elements.length < 2) {
        logger.warn("Not enough data. You've probably supplied a dataset with different format");
        return null;
      }
      return new Sentence(elements[0], elements[1]);
    }
  }

  public static class RemappedLabeledSentences extends LabeledSentences {
    private Map<String, String> mappings;

    public RemappedLabeledSentences(Map<String, String> mappings, DatasetMetaInfo info) throws URISyntaxException, IOException {
      super(info);
      this.mappings = mappings;
    }

    @Override
    public Sentence readNext() {
      Sentence sentence = super.readNext();
      if (sentence == null) {
        return null;
      }
      if (mappings.containsKey(sentence.label)) {
        sentence.label = mappings.get(sentence.label);
      }
      return sentence;
    }
  }
}
