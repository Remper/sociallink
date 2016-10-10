package eu.fbk.ict.fm.data.dataset;

import eu.fbk.ict.fm.data.DatasetMetaInfo;

import java.net.URISyntaxException;

/**
 * A binary file
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class Binary extends Dataset {
  public Binary(DatasetMetaInfo info) throws URISyntaxException {
    super(info);
  }

  @Override
  public void parse() {

  }
}
