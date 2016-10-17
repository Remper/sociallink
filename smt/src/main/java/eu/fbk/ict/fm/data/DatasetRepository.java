package eu.fbk.ict.fm.data;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import eu.fbk.ict.fm.data.dataset.*;
import eu.fbk.ict.fm.data.util.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/**
 * Class that manages locations of various repositories
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class DatasetRepository {
  private static final Logger logger = LoggerFactory.getLogger(DatasetRepository.class);

  Configuration conf;
  Map<String, DatasetMetaInfo> index = new HashMap<>();

  public DatasetRepository() throws IOException {
      this(new Configuration());
  }

  @Inject
  public DatasetRepository(Configuration configuration) throws IOException {
    this.conf = configuration;
    loadRepositoryIndex();
    if (!conf.getStorage().exists() && !conf.getStorage().mkdirs()) {
      throw new IOException("Can't create the storage directory");
    }
  }

  @SuppressWarnings("unchecked")
  private void loadRepositoryIndex() throws IOException {
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    Map<String, Object> rawIndex = mapper.readValue(conf.getRepository(), Map.class);
    for (Map.Entry<String, Object> dataset : rawIndex.entrySet()) {
      Map<String, Object> properties = (Map<String, Object>) dataset.getValue();

      //Parsing metadata for the object
      DatasetMetaInfo loc = new DatasetMetaInfo();
      loc.name = dataset.getKey();
      loc.type = (String) properties.get("type");
      loc.location = new URL((String) properties.get("location"));
      loc.size = ((Number) properties.getOrDefault("size", 0)).longValue();
      loc.compression = DatasetMetaInfo.Compression.valueOf((String) properties.get("compression"));
      if (loc.location.getProtocol().equals("file")) {
        loc.isOffline = true;
      }

      //Trying to find the file in cache
      File possibleOfflineLocation = loc.getOfflineLocation(conf.getStorage());
      loc.updateOffline(possibleOfflineLocation);
      index.put(loc.name, loc);
    }
  }

  public Dataset load(String name) throws Exception {
    if (!index.containsKey(name)) {
      //TODO: search the storage folder, if something comes up — add to the repository
      throw new Exception("Dataset \"" + name + "\" hasn't been found in the repository");
    }
    DatasetMetaInfo location = index.get(name);
    //See if we need to download the file
    if (!location.isOffline) {
      StringBuilder msg = new StringBuilder();
      msg.append("Dataset \"").append(name).append("\" hasn't been downloaded yet. Starting the download.");
      if (location.size > 0) {
        msg.append(" Expecting ").append(location.size).append(" bytes");
      }
      logger.info(msg.toString());
      Stopwatch watch = Stopwatch.start();
      File finalLocation = location.getOfflineLocation(conf.getStorage());
      copyFromURL(location.location, finalLocation);
      location.updateOffline(finalLocation);
      if (!location.isOffline) {
        logger.info("Download failed. " + (watch.click() / 1000) + "s elapsed");
        throw new Exception("Can't download the dataset \"" + name + "\"");
      }
      logger.info("Download finished. " + (watch.click() / 1000) + "s elapsed");
    }
    return instantiateDataset(location);
  }

  private Dataset instantiateDataset(DatasetMetaInfo location) throws Exception {
    Dataset target;
    logger.info("Starting loading the dataset " + location.name);
    Stopwatch watch = Stopwatch.start();
    switch (location.type) {
      case "labeledsentences":
        target = new LabeledSentences(location);
        break;
      case "csv":
      case "tsv":
        target = new CSVDataset(location);
        break;
      case "ngrams":
        target = new NGramMapping(location);
        break;
      case "features":
        target = new FeatureMapping(location);
        break;
      case "binary":
        target = new Binary(location);
        break;
      default:
        throw new Exception("The type \"" + location.type + "\" is not supported yet");
    }

    logger.info("Loaded dataset \"" + location.name + "\". " + (watch.click() / 1000) + "s elapsed");
    return target;
  }

  public static void copyFromURL(URL location, File file) throws IOException {
    if (location == null) {
      throw new IOException("Missing input resource!");
    }
    if (file.exists()) {
      file.delete();
    }
    InputStream in = location.openStream();
    FileOutputStream out = new FileOutputStream(file);
    byte[] buffer = new byte[1024];
    int len;
    while ((len = in.read(buffer)) != -1) {
      out.write(buffer, 0, len);
    }
    out.close();
    in.close();
    file.setReadOnly();
    file.setReadable(true, false);
  }
}
