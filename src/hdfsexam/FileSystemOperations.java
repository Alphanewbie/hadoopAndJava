package hdfsexam;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileSystemOperations {
  public void addFile(String source, String dest, Configuration conf) throws IOException {
    FileSystem fileSystem = FileSystem.get(conf);

    String filename = source.substring(source.lastIndexOf('/') + 1,source.length());

    if (dest.charAt(dest.length() - 1) != '/') {
      dest = dest + "/" + filename;
    } else {
      dest = dest + filename;
    }

    Path path = new Path(dest);
    if (fileSystem.exists(path)) {
      System.out.println("File " + dest + " already exists");
      return;
    }

    FSDataOutputStream out = fileSystem.create(path);
    InputStream in = new BufferedInputStream(new FileInputStream(new File(
        source)));

    byte[] b = new byte[1024];
    int numBytes = 0;
    while ((numBytes = in.read(b)) > 0) {
      out.write(b, 0, numBytes);
    }
    in.close();
    out.close();
    fileSystem.close();
  }

  public void readFile(String file, Configuration conf) throws IOException {
    FileSystem fileSystem = FileSystem.get(conf);

    Path path = new Path(file);
    if (!fileSystem.exists(path)) {
      System.out.println("File " + file + " does not exists");
      return;
    }

    FSDataInputStream in = fileSystem.open(path);

    String filename = file.substring(file.lastIndexOf('/') + 1,
        file.length());

    OutputStream out = new BufferedOutputStream(new FileOutputStream(
        new File(filename)));

    byte[] b = new byte[1024];
    int numBytes = 0;
    while ((numBytes = in.read(b)) > 0) {
      out.write(b, 0, numBytes);
    }

    in.close();
    out.close();
    fileSystem.close();
  }

  public void deleteFile(String file, Configuration conf) throws IOException {
    FileSystem fileSystem = FileSystem.get(conf);

    Path path = new Path(file);
    if (!fileSystem.exists(path)) {
      System.out.println("File " + file + " does not exists");
      return;
    }

    fileSystem.delete(new Path(file), true);

    fileSystem.close();
  }

  public void mkdir(String dir, Configuration conf) throws IOException {
    FileSystem fileSystem = FileSystem.get(conf);

    Path path = new Path(dir);
    if (fileSystem.exists(path)) {
      System.out.println("Dir " + dir + " already not exists");
      return;
    }

    fileSystem.mkdirs(path);

    fileSystem.close();
  }

  public static void main(String[] args) throws IOException {

	 // 정해진 기능이 완료되지 않고 종료 되는 것
    if (args.length < 1) {
      System.out.println("Usage: FileSystemOperations add/read/delete/mkdir"
          + " [<local_path> <hdfs_path>]");
      System.exit(1);
    }

    FileSystemOperations client = new FileSystemOperations();

    Configuration conf = new Configuration();
	conf.set("fs.defaultFS", "hdfs://192.168.111.120:9000");

    if (args[0].equals("add")) {
      if (args.length < 3) {
        System.out.println("Usage: hdfsclient add <local_path> "
            + "<hdfs_path>");
        System.exit(1);
      }

      client.addFile(args[1], args[2], conf);

    } else if (args[0].equals("read")) {
      if (args.length < 2) {
        System.out.println("Usage: hdfsclient read <hdfs_path>");
        System.exit(1);
      }

      client.readFile(args[1], conf);

    } else if (args[0].equals("delete")) {
      if (args.length < 2) {
        System.out.println("Usage: hdfsclient delete <hdfs_path>");
        System.exit(1);
      }

      client.deleteFile(args[1], conf);

    } else if (args[0].equals("mkdir")) {
      if (args.length < 2) {
        System.out.println("Usage: hdfsclient mkdir <hdfs_path>");
        System.exit(1);
      }

      client.mkdir(args[1], conf);

    } else {
      System.out.println("Usage: hdfsclient add/read/delete/mkdir"
          + " [<local_path> <hdfs_path>]");
      System.exit(1);
    }

    System.out.println("Done!");
  }
}
