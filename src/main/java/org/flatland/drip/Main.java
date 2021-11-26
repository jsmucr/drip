package org.flatland.drip;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This is meant to be the entry point of a forked child
 * that waits for the another entry point (the real meat)
 * and executes it.
 * The purpose is to have a pool of preforked processes
 * waiting in order to amortize the JVM startup time.
 * Processes have an "unique ID" assigned by parent that is meant
 * to be used to group them.
 * They have their own file as stdout and stderr and do NOT
 * have stdin.
 * Stdout is stdout.<unique id> and stderr is stderr.<unique id>.
 * The communication channel is a java.io.Scanner on the top
 * of a named pipe created by the parent. A standard named pipe
 * control.<unique id> in the working directory is used on
 * *NIX (man mkfifo) and \\.\pipe\preforkj.control.<unique id>
 * works for windows.
 */
public class Main {
    //! Control FIFO
    private final FileInputStream fifo;
    /**
     * Max running time in minutes. If 0 no maximum running time.
     * If the child real entry point does not exit within this
     * time the JVM instance is shut down.
     */
    private final int maxRunningTimeMinutes;

    //! Default maximum running time in minutes
    public static final int MAX_RUNNING_TIME_MINUTES = 5;

    public static void main(String[] args) throws Exception {
        new Main(args[0], args[1]).start();
    }

    public Main(String uniqueId, String workingDirectory) throws IOException {
        String currentOs = System.getProperty("os.name").toLowerCase();
        boolean isWindows = currentOs.contains("win");
        boolean isLinux = currentOs.contains("nux");
        if ((!isWindows) && (!isLinux))
            throw new UnsupportedOperationException("Unsupported OS");

        //! Unique JVM group instance ID assigned by parent
        //! Working directory
        File dir = new File(workingDirectory);
        // We don't rely on stdin and have our own stdout/err files
        System.in.close();
        System.out.close();
        System.err.close();
        System.setOut(new PrintStream(new File(dir, String.format("stdout.%s", uniqueId))));
        System.setErr(new PrintStream(new File(dir, String.format("stderr.%s", uniqueId))));

        if (isLinux) {
            // standard mkfifo on *NIX
            this.fifo = new FileInputStream(new File(dir, String.format("control.%s", uniqueId)));
        } else { // if (isWindows)
            // windows flavour
            // DO NOT USE File as, on windows, the client will NOT read bytes until the server end of
            // the pipe is closed
            this.fifo = new FileInputStream(String.format("\\\\.\\pipe\\preforkj.control.%s", uniqueId));
        }

        String idleTimeStr = System.getenv("DRIP_SHUTDOWN"); // in minutes
        this.maxRunningTimeMinutes = (idleTimeStr == null) ? MAX_RUNNING_TIME_MINUTES : Integer.parseInt(idleTimeStr);
        startIdleKiller();
    }

    private void killAfterTimeout() {
        try {
            Thread.sleep(this.maxRunningTimeMinutes * 60L * 1000L); // convert minutes to ms
        } catch (InterruptedException e) {
            System.err.println("drip: Interrupted timeout thread??");
            return; // I guess someone wanted to kill the timeout thread?
        }
        System.err.printf("Exiting after %d [min] timeout\n", this.maxRunningTimeMinutes);
        System.exit(0);
    }

    private void startIdleKiller() {
        if (this.maxRunningTimeMinutes != 0) {
            Thread idleKiller = new Thread(this::killAfterTimeout);

            idleKiller.setDaemon(true);
            idleKiller.start();
        }
    }

    public void start() throws Exception {
        // Will block until we get commands
        Scanner commandStream = new Scanner(this.fifo);
        // On windows if the parent exits (or disconnects the server pipe handle)
        // it is treated as an event that unblocks read, but of course we
        // don't have any data and a java.util.NoSuchElementException is generated
        // in the scanner. This is an annoying problem but I have no interest
        // in resolving it
        String mainClass = readString(commandStream);
        // Target program args separated by \u0000
        String mainArgs = readString(commandStream);
        // System properties separated by \u0000, i.e. -DA=B\u0000-DX=Y
        String runtimeArgs = readString(commandStream);
        // Environment variables separated by \u0000, i.e. A=B\u0000C=D
        String environment = readString(commandStream);
        commandStream.close();
        Method main = mainMethod(mainClass);
        mergeEnv(parseEnv(environment));
        setProperties(runtimeArgs);
        System.out.printf("%1$tT.%1$tL - Invoking Method '%2$s'\n", new Date(), main);
        Object retval = invoke(main, split(mainArgs, "\u0000"));
        System.out.printf("%1$tT.%1$tL - Method '%2$s' finished, returned '%3$s'\n", new Date(), main, retval);
    }

    private Method mainMethod(String className)
            throws ClassNotFoundException, NoSuchMethodException {
        if (className == null || className.equals(""))
            throw new ClassNotFoundException("No class name specified");

        return Class.forName(className, true, ClassLoader.getSystemClassLoader())
                .getMethod("main", String[].class);
    }

    private String[] split(String str, String delimiter) {
        if (str.length() == 0) {
            return new String[0];
        } else {
            try (Scanner s = new Scanner(str)) {
                s.useDelimiter(delimiter);

                LinkedList<String> list = new LinkedList<>();
                while (s.hasNext()) {
                    list.add(s.next());
                }
                return list.toArray(new String[0]);
            }
        }
    }

    private Object invoke(Method main, String[] args) throws Exception {
        return main.invoke(null, (Object) args);
    }

    private void setProperties(String runtimeArgs) {
        Matcher m = Pattern.compile("-D([^=]+)=([^\u0000]+)").matcher(runtimeArgs);

        while (m.find()) {
            System.setProperty(m.group(1), m.group(2));
        }
    }

    private Map<String, String> parseEnv(String str) {
        Map<String, String> env = new HashMap<>();

        for (String line : split(str, "\u0000")) {
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
        ((Map<String, String>) field.get(env)).putAll(newEnv);
        field.setAccessible(false);
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
