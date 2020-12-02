package org.flatland.drip;

import java.lang.System;
import java.lang.UnsupportedOperationException;
import java.lang.reflect.Method;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.io.*;
import java.util.Date;
import java.util.Map;
import java.util.HashMap;
import java.util.Scanner;
import java.util.LinkedList;
import java.util.List;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** This is meant to be the entry point of a forked child
 *  that waits for the another entry point (the real meat)
 *  and executes it.
 *  The purpose is to have a pool of preforked processes
 *  waiting in order to amortize the JVM startup time.
 *  Processes have an "unique ID" assigned by parent that is meant
 *  to be used to group them.
 *  They have their own file as stdout and stderr and do NOT
 *  have stdin.
 *  Stdout is stdout.<unique id> and stderr is stderr.<unique id>.
 *  The communication channel is a java.io.Scanner on the top
 *  of a named pipe created by the parent. A standard named pipe
 *  control.<unique id> in the working directory is used on
 *  *NIX (man mkfifo) and \\.\pipe\preforkj.control.<unique id>
 *  works for windows.
 */
public class Main
{
  private String mainClass;
  //! Working directory
  private File dir;
  //! Unique JVM group instance ID assigned by parent
  private String unique_id;
  //! Control FIFO
  private FileInputStream fifo;
  /** Max running time in minutes. If 0 no maximum running time.
   *  If the child real entry point does not exit within this
   *  time the JVM instance is shut down.
   */
  private int max_running_time_m;
  private Thread idle_killer;
  private boolean is_windows;
  private boolean is_linux;

  //! Default maximum running time in minutes
  public static final int MAX_RUNNING_TIME_M = 5;

  public static void main(String[] args) throws IOException, Exception
  {
    new Main(args[0], args[1]).start();
  }

  public Main(String unique_id, String working_directory) throws IOException
  {
    String current_os = System.getProperty("os.name").toLowerCase();
    this.is_windows = (current_os.indexOf("win") >= 0);
    this.is_linux = (current_os.indexOf("nux") >= 0);
    if ((!this.is_windows) && (!this.is_linux))
      throw new UnsupportedOperationException("Unsupported OS");

    this.unique_id = unique_id;
    this.dir = new File(working_directory);
    // We don't rely on stdin and have our own stdout/err files
    System.in.close();
    System.out.close();
    System.err.close();
    System.setOut(new PrintStream(new File(dir, String.format("stdout.%s", unique_id))));
    System.setErr(new PrintStream(new File(dir, String.format("stderr.%s", unique_id))));

    if (this.is_linux) {
      // standard mkfifo on *NIX
      this.fifo = new FileInputStream(new File(this.dir, String.format("control.%s", unique_id)));
    } else if (this.is_windows) {
      // windows flavour
      // DO NOT USE File as, on windows, the client will NOT read bytes until the server end of
      // the pipe is closed
      this.fifo = new FileInputStream(String.format("\\\\.\\pipe\\preforkj.control.%s", unique_id));
    }

    String idleTimeStr = System.getenv("DRIP_SHUTDOWN"); // in minutes
    this.max_running_time_m = (idleTimeStr == null) ? this.MAX_RUNNING_TIME_M : Integer.parseInt(idleTimeStr);
    startIdleKiller();
  }

  private void killAfterTimeout()
  {
    try {
      Thread.sleep(this.max_running_time_m * 60 * 1000); // convert minutes to ms
    } catch (InterruptedException e) {
      System.err.println("drip: Interrupted timeout thread??");
      return; // I guess someone wanted to kill the timeout thread?
    }
    System.err.printf("Exiting after %d [min] timeout\n", this.max_running_time_m);
    System.exit(0);
  }

  private void startIdleKiller()
  {
    if (this.max_running_time_m != 0) {
      this.idle_killer = new Thread() {
          public void run() {
            killAfterTimeout();
          }
        };

      this.idle_killer.setDaemon(true);
      this.idle_killer.start();
    }
  }

  public void start() throws Exception
  {
    // Will block until we get commands
    Scanner command_stream = new Scanner(this.fifo);
    // On windows if the parent exits (or disconnects the server pipe handle)
    // it is treated as an event that unblocks read, but of course we
    // don't have any data and a java.util.NoSuchElementException is generated
    // in the scanner. This is an annoying problem but I have no interest
    // in resolving it
    this.mainClass = readString(command_stream);
    // Target program args separated by \u0000
    String mainArgs = readString(command_stream);
    // System properties separated by \u0000, i.e. -DA=B\u0000-DX=Y
    String runtimeArgs = readString(command_stream);
    // Environment variables separated by \u0000, i.e. A=B\u0000C=D
    String environment = readString(command_stream);
    command_stream.close();
    Method main = mainMethod(mainClass);
    mergeEnv(parseEnv(environment));
    setProperties(runtimeArgs);
    System.out.printf("%1$tT.%1$tL - Invoking Method '%2$s'\n", new Date(), main);
    Object retval = invoke(main, split(mainArgs, "\u0000"));
    System.out.printf("%1$tT.%1$tL - Method '%2$s' finished, returned '%3$s'\n", new Date(), main, retval);
  }

  private Method mainMethod(String className)
    throws ClassNotFoundException, NoSuchMethodException
  {
    if (className == null || className.equals(""))
      throw new ClassNotFoundException("No class name specified");

    return Class.forName(className, true, ClassLoader.getSystemClassLoader())
      .getMethod("main", String[].class);
  }

  private String[] split(String str, String delim)
  {
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

  private Object invoke(Method main, String[] args) throws Exception
  {
    return main.invoke(null, (Object)args);
  }

  private void setProperties(String runtimeArgs)
  {
    Matcher m = Pattern.compile("-D([^=]+)=([^\u0000]+)").matcher(runtimeArgs);

    while (m.find()) {
      System.setProperty(m.group(1), m.group(2));
    }
  }

  private Map<String, String> parseEnv(String str)
  {
    Map<String, String> env = new HashMap<String, String>();

    for (String line: split(str, "\u0000")) {
      String[] var = line.split("=", 2);
      env.put(var[0], var[1]);
    }
    return env;
  }

  @SuppressWarnings("unchecked")
  private void mergeEnv(Map<String, String> newEnv)
    throws NoSuchFieldException, IllegalAccessException
  {
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

  private static final Pattern EVERYTHING = Pattern.compile(".+", Pattern.DOTALL);
  private String readString(Scanner s) throws IOException
  {
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
