package eu.fbk.ict.fm.data.util;

/**
 * A simple class that provide a simple time measuring
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class Stopwatch {
  private long start = 0;
  private long firstStart = 0;
  private long lastLapTime = 0;

  private Stopwatch() {
  }

  public static Stopwatch start() {
    Stopwatch stopwatch = new Stopwatch();
    stopwatch.reset();
    return stopwatch;
  }

  public void reset() {
    firstStart = start = System.currentTimeMillis();
  }

  public long click() {
    long end = System.currentTimeMillis();
    lastLapTime = end - start;
    start = System.currentTimeMillis();
    return lastLapTime;
  }

  public long getLastLapTime() {
    return lastLapTime;
  }

  public long getTimeSinceStart() {
    return System.currentTimeMillis() - firstStart;
  }
}
