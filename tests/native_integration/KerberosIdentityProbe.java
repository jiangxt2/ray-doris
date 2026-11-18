import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

public class KerberosIdentityProbe {
  public static void main(String[] args) throws Exception {
    Configuration base = new Configuration();
    UserGroupInformation.setConfiguration(base);
    Configuration aliceConfig = new Configuration(base), bobConfig = new Configuration(base);
    aliceConfig.set("hadoop.security.kerberos.ticket.cache.path", "/fixture/alice.cache");
    bobConfig.set("hadoop.security.kerberos.ticket.cache.path", "/fixture/bob.cache");
    URI uri = URI.create("hdfs://hdfs-probe:8020");
    FileSystem alice = FileSystem.get(uri, aliceConfig, args[0]);
    FileSystem bob = FileSystem.get(uri, bobConfig, args[1]);
    Path file = new Path("/alice/input.txt");
    try (FSDataInputStream stream = alice.open(file)) {
      if (stream.read() != 'i') throw new RuntimeException("alice data mismatch");
    }
    boolean denied = false;
    try (FSDataInputStream stream = bob.open(file)) { stream.read(); }
    catch (IOException error) {
      if (!error.toString().contains("AccessControlException")) throw error;
      denied = true;
    }
    if (!denied) throw new RuntimeException("bob accessed alice data");
    try (FSDataInputStream stream = alice.open(file)) {
      if (stream.read() != 'i') throw new RuntimeException("alice after bob mismatch");
    }
    System.out.println("same-JVM HDFS: alice allowed, bob denied, alice still allowed");
  }
}
