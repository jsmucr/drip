package org.flatland.drip;
import java.io.*;

// not safe to use flip() while any other threads are accessing this object in any way
public class SwitchableOutputStream extends OutputStream implements Switchable {
  private final File path;

  private OutputStream out;
  private boolean switched;

  public SwitchableOutputStream(OutputStream old, File path) {
    this.out = old;
    this.path = path;
    this.switched = false;
  }

  public File path() {
    return path;
  }

  public void flip() throws IllegalStateException, IOException {
    if (switched) {
      throw new IllegalStateException("Already switched to secondary output");
    }
    switched = true;
    out.close();

    out = new FileOutputStream(path);
  }

  public void write(int b) throws IOException {
    out.write(b);
  }

  public void write(byte[] b) throws IOException {
    out.write(b);
  }

  public void write(byte[] b, int off, int len) throws IOException {
    out.write(b, off, len);
  }

  public void close() throws IOException {
    out.close();
  }

  public FileDescriptor getFD() throws IOException {
    return ((FileOutputStream) out).getFD();
  }
}
