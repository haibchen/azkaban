package azkaban.cluster;

import azkaban.utils.UndefinedPropertyException;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;

/**
 * Unit tests for {@link ClusterLoader}.
 */
public class ClusterLoaderTest {
  @Rule
  public TemporaryFolder testDir = new TemporaryFolder();

  @Test
  public void testLoadingSingleCluster() throws IOException {
    File clusterDir = testDir.newFolder("single-cluster");
    File clusterConfig = new File(clusterDir, "cluster.properties");
    try (Writer fileWriter = new OutputStreamWriter(
        new FileOutputStream(clusterConfig), StandardCharsets.UTF_8)) {
      fileWriter.write("hadoop.security.manager.class=azkaban.security.HadoopSecurityManager_H_2_0\n");
      fileWriter.write("A=a\n");
      fileWriter.write("B=b\n");
    }
    ClusterRegistry clusterRegistry = new ClusterRegistry();

    ClusterLoader.loadCluster(clusterDir, clusterRegistry);

    Cluster cluster = clusterRegistry.getCluster(clusterDir.getName());
    Assert.assertEquals(clusterDir.getName(), cluster.clusterId);
  }

  @Test (expected = FileNotFoundException.class)
  public void testLoadingSingleClusterWithMissingClusterConfig() throws IOException {
    File clusterDir = testDir.newFolder("single-cluster-no-config");
    ClusterRegistry clusterRegistry = new ClusterRegistry();

    ClusterLoader.loadCluster(clusterDir, clusterRegistry);
  }

  @Test
  public void testLoadingMultipleClusters() throws IOException {
    File clustersDir = new File(getClass().getClassLoader().getResource("clusters").getFile());
    ClusterRegistry clusterRegistry = new ClusterRegistry();

    ClusterLoader clusterLoader = new ClusterLoader(clustersDir, clusterRegistry);

    Cluster defaultCluster = clusterRegistry.getCluster("default");
    Assert.assertEquals("default", defaultCluster.clusterId);
    Cluster another = clusterRegistry.getCluster("another");
    Assert.assertEquals("another", another.clusterId);
  }
}
