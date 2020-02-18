package azkaban.jobExecutor;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;

import static com.google.common.base.Charsets.UTF_8;

/**
 * Unit tests for {@link JobClassLoader}.
 */
public class JobClassLoaderTest {
  private final static String RESOURCE_FILE = "resource.txt";
  private static final String SAMPLE_JAR = "helloworld.jar";
  @Rule
  public TemporaryFolder testDir = new TemporaryFolder();

  @Test
  public void testGetResource() throws IOException {
    URL testJar = makeTestJar().toURI().toURL();

    ClassLoader currentClassLoader = getClass().getClassLoader();
    ClassLoader jobClassLoader = new JobClassLoader(
        new URL[] { testJar }, currentClassLoader, "testJob");

    Assert.assertNull("Resource should not be found in the parent classloader",
        currentClassLoader.getResource(RESOURCE_FILE));
    Assert.assertNotNull("Resource should be found in JobClassLoader",
        jobClassLoader.getResource(RESOURCE_FILE));
  }

  private File makeTestJar() throws IOException {
    File jarFile = testDir.newFile("test.jar");
    try (JarOutputStream out = new JarOutputStream(new FileOutputStream(jarFile))) {
      ZipEntry entry = new ZipEntry(RESOURCE_FILE);
      out.putNextEntry(entry);
      out.write("hello".getBytes(UTF_8));
      out.closeEntry();
    }
    return jarFile;
  }

  @Test (expected = ClassNotFoundException.class)
  public void testNonexistClass() throws ClassNotFoundException {
    ClassLoader currentClassLoader = getClass().getClassLoader();
    ClassLoader jobClassLoader = new JobClassLoader(
        new URL[] {}, currentClassLoader, "testJob");

    Assert.assertNull("This class does not exist",
        jobClassLoader.loadClass("nonexistent.class.name"));
  }

  @Test
  public void testClassAvailableInParentClassLoader() throws ClassNotFoundException {
    ClassLoader currentClassLoader = getClass().getClassLoader();
    ClassLoader jobClassLoader = new JobClassLoader(
        new URL[] {}, currentClassLoader, "testJob");
    Class clazz = jobClassLoader.loadClass(JobClassLoaderTest.class.getName());
    Assert.assertEquals(currentClassLoader, clazz.getClassLoader());
  }

  @Test
  public void testClassAvailableInJobClassLoader()
      throws MalformedURLException, ClassNotFoundException {
    ClassLoader currentClassLoader = getClass().getClassLoader();

    File helloworldJar = new File(currentClassLoader.getResource(SAMPLE_JAR).getFile());
    URL helloworlURL = helloworldJar.toURI().toURL();

    ClassLoader jobClassLoader = new JobClassLoader(
        new URL[] {helloworlURL}, currentClassLoader, "testJob");

    Class clazz = jobClassLoader.loadClass("org.hello.world.HelloWorld");
    Assert.assertEquals(jobClassLoader, clazz.getClassLoader());
  }
}
