package org.flatland.drip;

import java.io.*;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.Permission;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Main {

    private static class DripExitException extends SecurityException {
        public void printStackTrace(PrintStream out) {
            throw this;
        }
        public void reallyPrintStackTrace(PrintStream out) {
            super.printStackTrace(out);
        }
    }

    private class DripSecurityManager extends SecurityManager {

        private final SecurityManager base;

        DripSecurityManager(SecurityManager base) {
            this.base = base;
        }

        @Override
        public void checkExit(int status) {
            if (base != null) {
                base.checkExit(status);
            }
            if (!exiting) {
                throw new DripExitException();
            }
        }

        @Override
        public void checkPermission(Permission perm) {
            if (base != null) {
                base.checkPermission(perm);
            }
        }
    }

    private boolean exiting;

    private final String mainClass;
    private final File dir;
    private SwitchableOutputStream err;
    private SwitchableOutputStream out;
    private SwitchableInputStream in;

    public Main(String mainClass, String dir) {
        this.mainClass = mainClass;
        this.dir = new File(dir);
        System.setSecurityManager(new DripSecurityManager(System.getSecurityManager()));
    }

    private void exit(int code) {
        exiting = true;
        System.exit(code);
    }

    private void killAfterTimeout() {
        String idleTimeStr = System.getenv("DRIP_SHUTDOWN"); // in minutes
        long idleTime;
        if (idleTimeStr == null) {
            idleTime = 4 * 60; // four hours
        } else {
            idleTime = Integer.parseInt(idleTimeStr);
        }

        try {
            Thread.sleep(idleTime * 60 * 1000); // convert minutes to ms
        } catch (InterruptedException e) {
            System.err.println("drip: Interrupted??");
            return; // I guess someone wanted to kill the timeout thread?
        }

        File lockDir = new File(dir, "lock");
        if (lockDir.mkdir()) {
            exit(0);
        } else {
            // someone is already connected; let the process finish
        }
    }

    private void startIdleKiller() {
        Thread idleKiller = new Thread(this::killAfterTimeout);

        idleKiller.setDaemon(true);
        idleKiller.start();
    }

    public static void main(String[] args) throws Exception {
        new Main(args[0], args[1]).start();
    }

    public void start() throws Exception {
        reopenStreams();

        Method main = mainMethod(mainClass);
        Method init = mainMethod(System.getenv("DRIP_INIT_CLASS"));
        String initArgs = System.getenv("DRIP_INIT");
        if (initArgs != null) {
            invoke(init == null ? main : init, split(initArgs, "\n"));
        }
        startIdleKiller();

        Scanner fromBash = new Scanner(new File(dir, "control"));
        String mainArgs = readString(fromBash);
        String runtimeArgs = readString(fromBash);
        String environment = readString(fromBash);
        fromBash.close();

        mergeEnv(parseEnv(environment));
        setProperties(runtimeArgs);
        switchStreams();


        try {
            long start = System.currentTimeMillis();
            try {
                invoke(main, split(mainArgs, "\u0000"));
            } catch (InvocationTargetException invocationTargetException) {
                throw invocationTargetException.getCause();
            } finally {
                System.err.println("Spent time running: " + (System.currentTimeMillis() - start));
            }
        } catch (DripExitException ignored) {

        } catch (Throwable t) {
            System.err.println(main + " exited with an exception.");
            t.printStackTrace(System.err);
        }
    }

    private Method mainMethod(String className)
            throws ClassNotFoundException, NoSuchMethodException {
        if (className == null || className.equals("")) {
            return null;
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

            LinkedList<String> list = new LinkedList<>();
            while (s.hasNext()) {
                list.add(s.next());
            }
            return list.toArray(new String[0]);
        }
    }

    private void invoke(Method main, String[] args) throws Exception {
        main.invoke(null, (Object) args);
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

    @SuppressWarnings("unchecked")
    static void replaceFileDescriptor(FileDescriptor a, FileDescriptor b)
            throws NoSuchFieldException, IllegalAccessException {
        Field field = FileDescriptor.class.getDeclaredField("fd");
        field.setAccessible(true);
        field.set(a, field.get(b));
        field.setAccessible(false);
    }

    private void flip(Switchable s) throws IllegalStateException, IOException {
        while (!s.path().exists()) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException ignored) {
            }
        }
        s.flip();
    }

    private void reopenStreams() {
        this.in = new SwitchableInputStream(System.in, new File(dir, "in"));
        this.out = new SwitchableOutputStream(System.out, new File(dir, "out"));
        this.err = new SwitchableOutputStream(System.err, new File(dir, "err"));

        System.setIn(new BufferedInputStream(in));
        System.setOut(new PrintStream(out));
        System.setErr(new PrintStream(err));
    }

    private void switchStreams() throws Exception {
        flip(in);
        flip(out);
        flip(err);

        replaceFileDescriptor(FileDescriptor.in, this.in.getFD());
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