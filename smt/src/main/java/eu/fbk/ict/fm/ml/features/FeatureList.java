package eu.fbk.ict.fm.ml.features;

import org.fbk.cit.hlt.core.util.HashIndexSet;
import org.fbk.cit.hlt.core.util.IndexSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Writer;
import java.util.*;

/**
 * Ranks words according their frequency.
 *
 * @author Claudio Giuliano (giuliano@fbk.eu)
 */
public class FeatureList {
	private static final Logger logger = LoggerFactory.getLogger(FeatureList.class);

	private SortedMap<Double, Set<Entry>> scoreMap;
	private Map<String, Double> featureMap;
	private IndexSet<String> indexSet;

	public FeatureList() {
		scoreMap = new TreeMap<>(new DoubleComparator());
		// feature weight
		featureMap = new HashMap<>();
		//feature index, starting from 1 for libsvm/liblinear
		indexSet = new HashIndexSet<>(1);
	}

	public void put(String w, double d) {
		featureMap.put(w, d);
		indexSet.add(w);
    int index = indexSet.getIndex(w);

		Set<Entry> set = scoreMap.get(d);
		if (set == null) {
			set = new HashSet<>();
			scoreMap.put(d, set);
		}

		set.add(new Entry(index, w, d));
	}

	public Double getWeight(String feature) {
		return featureMap.get(feature);
	}

	public Integer getIndex(String feature) {
		return indexSet.getIndex(feature);
	}

	//
	public Set<Entry> first() {
		if (scoreMap.size() == 0) {
			return new HashSet<>();
		}

		Double d = scoreMap.firstKey();
		return scoreMap.get(d);
	}


	private Set<Entry> get(Double d) {
		if (scoreMap.size() == 0) {
			return new HashSet<>();
		}

		return scoreMap.get(d);
	}

	public int size() {
		return scoreMap.size();
	}

	public Iterator<Entry> iterator() {
		return toList().iterator();
	}

  public Set<String> toSet() {
    return toSet(0);
  }

	public Set<String> toSet(int k) {
		Set<String> set = new HashSet<>();

    for (Set<Entry> entries : scoreMap.values()) {
      for (Entry entry : entries) {
        set.add(entry.key());
        if (k != 0 && set.size() >= k) {
          return set;
        }
      }
    }

		return set;
	}

  public List<Entry> toList() {
    return toList(0);
  }

	public List<Entry> toList(int k) {
    int i = 0;
    List<Entry> list = new ArrayList<>();
		for (Set<Entry> entry : scoreMap.values()) {
			list.addAll(entry);

      if (k != 0 && ++i >= k) {
        return list;
      }
		}

		return list;
	}

	public String toString(int n) {
		StringBuilder sb = new StringBuilder();
		int i = 0;
		for (Map.Entry<Double, Set<Entry>> entries : scoreMap.entrySet()) {
			for (Entry entry : entries.getValue()) {
				sb.append((++i));
				sb.append("\t");
				sb.append(entries.getKey());
				sb.append("\t");
				sb.append(entry);
				sb.append("\n");
			}

      if (n != 0 && i > n) {
        break;
      }
    }
		return sb.toString();
	}

  public String toString() {
    return toString(0);
  }

	public void write(Writer writer) throws IOException {
    for (Map.Entry<Double, Set<Entry>> entries : scoreMap.entrySet()) {
      for (Entry entry : entries.getValue()) {
        writer.append(entries.getKey().toString());
        writer.append("\t");
        writer.append(entry.toString());
        writer.append("\n");
      }
    }

		writer.flush();
	}

	public void writeIndex(Writer writer) throws IOException {
    for (String feature : indexSet) {
      int index = indexSet.getIndex(feature);
      double score = featureMap.get(feature);
      writer.write(index + "\t" + feature + "\t" + score + "\n");
    }

		writer.flush();
	}

	public class Entry {
		private String instance;
		private double score;
		private int index;

		Entry(int index, String instance, double score) {
			this.instance = instance;
			this.score = score;
      this.index = index;
		}

		public String key() {
			return instance;
		}

		public double score() {
			return score;
		}

    public int index() {
      return index;
    }

    public String toString() {
			return index + "\t" + instance + "\t" + score;
		}

	}

	private class DoubleComparator implements Comparator {
		public int compare(Object o1, Object o2) {
			Double d1 = (Double) o1;
			Double d2 = (Double) o2;
			double diff = d2 - d1;

			if (diff == 0) {
				return 0;
			}
			else if (diff < 0) {
				return -1;
			}

			return 1;
		}

		public boolean equals(Object obj) {
			return false;
		}
	}
}
