package eu.fbk.ict.fm.data.dataset;

import eu.fbk.ict.fm.data.DatasetMetaInfo;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.LineNumberReader;
import java.net.URISyntaxException;
import java.util.Iterator;

/**
 * A CSV based dataset that reads contents of CSV one line at a time
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class CSVDataset extends Dataset implements Closeable {
  final static Logger logger = LoggerFactory.getLogger(CSVDataset.class);
  final static char SPLIT_CHARACTER_TSV = '\t';
  final static char SPLIT_CHARACTER_CSV = ',';

  protected LineNumberReader reader;
  protected Iterator<CSVRecord> records;
  protected CSVParser parser;
  protected char splitCharacter;

  public CSVDataset(DatasetMetaInfo info) throws URISyntaxException, IOException {
    super(info);
    splitCharacter = SPLIT_CHARACTER_CSV;
    if (info.type.equals("tsv")) {
      splitCharacter = SPLIT_CHARACTER_TSV;
    }
    init();
  }

  private void init() throws IOException {
    reader = getReader();
    parser = new CSVParser(reader, CSVFormat.DEFAULT.withDelimiter(splitCharacter).withHeader());
    records = parser.iterator();
  }

  public void reopen() {
    try {
      close();
      init();
    } catch (IOException e) {
      logger.error(e.getMessage(), e);
    }
  }

  public CSVRecord readNext() {
    if (!records.hasNext()) {
      return null;
    }
    return records.next();
  }

  @Override
  public void close() throws IOException {
    parser.close();
    reader.close();
  }

  @Override
  public void parse() {}
}
