package eu.fbk.ict.fm.data;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;

/**
 * A configuration object to load and store the datasets
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class Configuration {
  protected URL repository = Configuration.class.getResource("/data/repository.yml");
  protected File storage = new File(System.getProperty("user.home") + "/.fbkdata");

  public void loadFromProperties(Properties properties) throws MalformedURLException {
    if (properties.contains("repository")) {
      this.repository = new URL(properties.getProperty("repository"));
    }
    if (properties.contains("storage")) {
      this.storage = new File(properties.getProperty("storage"));
    }
  }

  public URL getRepository() {
    return repository;
  }

  public void setRepository(URL repository) {
    this.repository = repository;
  }

  public File getStorage() {
    return storage;
  }

  public void setStorage(File storage) {
    this.storage = storage;
  }
}
