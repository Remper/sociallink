package eu.fbk.ict.fm.data.dataset;

import eu.fbk.ict.fm.data.DatasetMetaInfo;

import java.io.*;
import java.net.URISyntaxException;
import java.util.zip.GZIPInputStream;

/**
 * The main entry point for downloading and loading into memory the datasets
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public abstract class Dataset {
  protected DatasetMetaInfo info;
  protected File source;

  public Dataset(DatasetMetaInfo info) throws URISyntaxException {
    assert info.isOffline;
    this.info = info;
    this.source = new File(this.info.location.toURI());
    parse();
  }

  public abstract void parse();

  protected LineNumberReader getReader() throws IOException {
    return new LineNumberReader(getRawReader());
  }

  public Reader getRawReader() throws IOException {
    InputStream stream = new FileInputStream(source);
    switch (this.info.compression) {
      case GZ:
        stream = new GZIPInputStream(stream);
        break;
      case PLAIN:
        //No need to modify stream here
        break;
    }
    return new InputStreamReader(stream);
  }

  public DatasetMetaInfo getInfo() {
    return info;
  }

  public File getSource() {
    return source;
  }
}
