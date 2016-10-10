package eu.fbk.ict.fm.data;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Meta info for the dataset. Direct mapping from the repository.yml
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class DatasetMetaInfo {
  public URL location;
  public long size = 0L;
  public String name;
  public String type;
  public Compression compression = Compression.PLAIN;
  public boolean isOffline = false;

  public File getOfflineLocation(File storage) {
    return new File(storage, name);
  }

  public void updateOffline(File file) throws MalformedURLException {
    if (file.exists()) {
      location = file.toURI().toURL();
      isOffline = true;
    }
  }

  public enum Compression {
    PLAIN, GZ;
  }
}