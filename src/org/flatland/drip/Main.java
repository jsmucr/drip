package org.flatland.drip;
import java.lang.reflect.Method;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.io.*;
import java.util.Map;
import java.util.HashMap;
import java.util.Scanner;
import java.util.LinkedList;
import java.util.List;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Main {
  private String mainClass;
  //! Working directory
  private File dir;
  //! Unique JVM instance ID assigned by parent
  private String unique_id;
  //! Control FIFO
  private File fifo;
  //! Max idle time in minutes. If 0 no maximum idle time
  private int idle_time_m;
  private SwitchableOutputStream err;
  private SwitchableOutputStream out;
  private SwitchableInputStream  in;

  //! Default idle time in minutes
  public static final int IDLE_TIME_M = 240;

  public static void main(String[] args) throws IOException, Exception
  {
    new Main(args[0], args[1]).start();
  }

  public Main(String unique_id, String working_directory) throws IOException
  {
    this.unique_id = unique_id;
    this.dir = new File(working_directory);
    // We don't rely on stdin/out/err
    System.in.close();
    System.out.close();
    System.err.close();
    String idleTimeStr = System.getenv("DRIP_SHUTDOWN"); // in minutes
    idleTimeStr == null ? this.idle_time_m = this.IDLE_TIME_M : this.idle_time_m = Integer.parseInt(idleTimeStr);
  }

  private void killAfterTimeout() {
    try {
      Thread.sleep(this.idle_time_m * 60 * 1000); // convert minutes to ms
    } catch (InterruptedException e) {
      System.err.println("drip: Interrupted timeout thread??");
      return; // I guess someone wanted to kill the timeout thread?
    }

    System.exit(0);
  }

  private void startIdleKiller() {
    Thread idleKiller = new Thread() {
        public void run() {
          killAfterTimeout();
        }
      };

    idleKiller.setDaemon(true);
    idleKiller.start();
  }

  public void start() throws Exception {
    reopenStreams();

    // Will block until we get commands
    Scanner fromBash = new Scanner(this.fifo);
    this.mainClass = readString(fromBash);
    // Target program args separated by \u0000
    String mainArgs = readString(fromBash);
    // System properties separated by \u0000, i.e. -DA=B\u0000-DX=Y
    String runtimeArgs = readString(fromBash);
    // Environment variables separated by \u0000, i.e. A=B\u0000C=D
    String environment = readString(fromBash);
    fromBash.close();
    Method main = mainMethod(mainClass);
    mergeEnv(parseEnv(environment));
    setProperties(runtimeArgs);
//     switchStreams();
    startIdleKiller();
    invoke(main, split(mainArgs, "\u0000"));
  }

  private Method mainMethod(String className)
    throws ClassNotFoundException, NoSuchMethodException
  {
    if (className == null || className.equals("")) {
      throw new ClassNotFoundException("No class name specified");
    } else {
      return Class.forName(className, true, ClassLoader.getSystemClassLoader())
        .getMethod("main", String[].class);
    }
  }

  private String[] split(String str, String delim) {
    if (str.length() == 0) {
      return new String[0];
    } else {
      Scanner s = new Scanner(str);
      s.useDelimiter(delim);

      LinkedList<String> list = new LinkedList<String>();
      while (s.hasNext()) {
        list.add(s.next());
      }
      return list.toArray(new String[0]);
    }
  }

  private void invoke(Method main, String[] args) throws Exception {
    main.invoke(null, (Object)args);
  }

  private void setProperties(String runtimeArgs) {
    Matcher m = Pattern.compile("-D([^=]+)=([^\u0000]+)").matcher(runtimeArgs);

    while (m.find()) {
      System.setProperty(m.group(1), m.group(2));
    }
  }

  private Map<String, String> parseEnv(String str) {
    Map<String, String> env = new HashMap<String, String>();

    for (String line: split(str, "\u0000")) {
      String[] var = line.split("=", 2);
      env.put(var[0], var[1]);
    }
    return env;
  }

  @SuppressWarnings("unchecked")
  private void mergeEnv(Map<String, String> newEnv)
    throws NoSuchFieldException, IllegalAccessException {
    Map<String, String> env = System.getenv();
    Class<?> classToHack = env.getClass();
    if (!(classToHack.getName().equals("java.util.Collections$UnmodifiableMap"))) {
      throw new RuntimeException("Don't know how to hack " + classToHack);
    }

    Field field = classToHack.getDeclaredField("m");
    field.setAccessible(true);
    ((Map<String,String>)field.get(env)).putAll(newEnv);
    field.setAccessible(false);
  }

  @SuppressWarnings("unchecked")
  static void replaceFileDescriptor(FileDescriptor a, FileDescriptor b)
    throws NoSuchFieldException, IllegalAccessException {
    Field field = FileDescriptor.class.getDeclaredField("fd");
    field.setAccessible(true);
    field.set(a, field.get(b));
    field.setAccessible(false);
  }

  private void flip(Switchable s) throws IllegalStateException, IOException {
    while (! s.path().exists()) {
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
      }
    }
    s.flip();
  }

  private void reopenStreams() throws FileNotFoundException, IOException {
    this.fifo = new File(this.dir, String.format("control.%s", unique_id));
    System.setOut(new PrintStream(new File(dir, String.format("stdout.%s", unique_id))));
    System.setErr(new PrintStream(new File(dir, String.format("stderr.%s", unique_id))));

//     this.in  = new SwitchableInputStream(
//         System.in, new File(dir, String.format("stdin.%s", unique_id)
//     );
//     this.out = new SwitchableOutputStream(
//         System.out, new File(dir, String.format("stdout.%s", unique_id)
//     );
//     this.err = new SwitchableOutputStream(
//         System.err, new File(dir, String.format("stderr.%s", unique_id)
//     );

//     System.setIn(new BufferedInputStream(in));
//     System.setOut(new PrintStream(out));
//     System.setErr(new PrintStream(err));
  }

  private void switchStreams() throws Exception {
    flip(in);
    flip(out);
    flip(err);

    replaceFileDescriptor(FileDescriptor.in,  this.in.getFD());
    replaceFileDescriptor(FileDescriptor.out, this.out.getFD());
    replaceFileDescriptor(FileDescriptor.err, this.err.getFD());
  }

  private static final Pattern EVERYTHING = Pattern.compile(".+", Pattern.DOTALL);
  private String readString(Scanner s) throws IOException {
    s.useDelimiter(":");
    int numChars = s.nextInt();
    s.skip(":");

    String arg;
    if (numChars == 0) { // horizon treats 0 as "unbounded"
      arg = "";
    } else {
      arg = s.findWithinHorizon(EVERYTHING, numChars);
      if (arg.length() != numChars) {
        throw new IOException("Expected " + numChars + " characters but found only " + arg.length() + " in string: \"" + arg + "\"");
      }
    }

    String terminator = s.findWithinHorizon(",", 1);
    if (!(terminator.equals(","))) {
      throw new IOException("Instead of comma terminator after \"" + arg + "\", found " + terminator);
    }
    return arg;
  }
}
