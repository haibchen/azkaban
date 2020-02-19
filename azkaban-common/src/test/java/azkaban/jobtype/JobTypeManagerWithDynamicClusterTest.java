/*
 * Copyright 2020 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package azkaban.jobtype;

import azkaban.cluster.Cluster;
import azkaban.cluster.ClusterRegistry;
import azkaban.cluster.ClusterRouter;
import azkaban.cluster.DefaultClusterRouter;
import azkaban.flow.CommonJobProperties;
import azkaban.jobExecutor.Job;
import azkaban.utils.Props;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;

import static azkaban.test.Utils.initServiceProvider;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Unit tests for {@link JobTypeManager} when clusters are configured.
 */
public class JobTypeManagerWithDynamicClusterTest {
  private static final Logger LOG = Logger.getLogger(JobTypeManagerTest.class);
  private static final String HADOOP_SECURITY_MANAGER_CLASS =
      "azkaban.security.HadoopSecurityManager_H_2_0";

  public File testDirectory;
  private Cluster defaultCluster;
  private String testPluginDirPath;
  private ClusterRouter clusterRouter;

  public JobTypeManagerWithDynamicClusterTest() throws Exception {
    this.testDirectory = Files.createTempDir();
    this.defaultCluster = createDefaultCluster();

    ClusterRegistry clusterRegistry = new ClusterRegistry();
    clusterRegistry.addCluster(defaultCluster.clusterId, defaultCluster);
    this.clusterRouter = new DefaultClusterRouter(clusterRegistry);

    this.testPluginDirPath = Resources.getResource("plugins/jobtypes").getPath();
  }

  private Cluster createDefaultCluster() throws IOException {
    Props clusterProps = new Props();
    File hadoopJar = makeTestJar( "hadoop", "hadoop.jar");
    clusterProps.put(Cluster.LIBRARY_PATH_PREFIX + "hadoop", hadoopJar.getParentFile().getPath());
    clusterProps.put(Cluster.NATIVE_LIBRARY_PATH_PREFIX + "hadoop", "hadoop-native-lib");
    File hiveJar = makeTestJar("hive", "hive.jar");
    clusterProps.put(Cluster.LIBRARY_PATH_PREFIX + "hive", hiveJar.getParentFile().getPath());
    clusterProps.put(Cluster.NATIVE_LIBRARY_PATH_PREFIX + "hive", "hive-native-lib");
    clusterProps.put(Cluster.HADOOP_SECURITY_MANAGER_CLASS_PROP, HADOOP_SECURITY_MANAGER_CLASS);

    return new Cluster("default",  clusterProps);
  }

  private File makeTestJar(String folderName, String jarName)
      throws IOException {
    File folder = new File(this.testDirectory, folderName);
    folder.mkdirs();
    File jarFile = new File(folder, jarName);
    try (JarOutputStream out = new JarOutputStream(new FileOutputStream(jarFile))) {
      ZipEntry entry = new ZipEntry("resource.txt");
      out.putNextEntry(entry);
      out.write("hello".getBytes(UTF_8));
      out.closeEntry();
    }
    return jarFile;
  }

  @Before
  public void setUp() throws Exception {
    initServiceProvider();
  }

  /**
   * Unit test of JobTypeManager's setup for jobs without 'job.dependency.components' specified.
   */
  @Test
  public void testJobTypeManagerJobSetupWithoutJobComponentDependency() {
    JobTypeManager manager = new JobTypeManager(this.testPluginDirPath, null,
        this.getClass().getClassLoader(), this.clusterRouter);

    Props jobProps = new Props();
    jobProps.put("type", "anothertestjob");
    jobProps.put("propB", "b");
    Job job = manager.buildJobExecutor("anothertestjob", jobProps, LOG);

    jobProps = ((FakeJavaJob) job).getJobProps();
    Assert.assertEquals(
        defaultCluster.clusterId, jobProps.getString(CommonJobProperties.TARGET_CLUSTER));

    String clusterClassPath = jobProps.getString(CommonJobProperties.TARGET_CLUSTER_CLASSPATH);
    Assert.assertFalse(clusterClassPath.contains("hive"));
    Assert.assertTrue(clusterClassPath.contains("hadoop"));

    String jvmArgs = jobProps.getString(CommonJobProperties.TARGET_CLUSTER_NATIVE_LIB);
    Assert.assertFalse(jvmArgs.contains("hive"));
    Assert.assertTrue(jvmArgs.contains("hadoop"));
  }

  /**
   * Unit test of JobTypeManager's setup for jobs with 'job.dependency.components' specified.
   */
  @Test
  public void testJobTypeManagerJobSetupWithJobComponentDependency() {
    JobTypeManager manager = new JobTypeManager(this.testPluginDirPath, null,
        this.getClass().getClassLoader(), this.clusterRouter);

    Props jobProps = new Props();
    jobProps.put("type", "anothertestjob");
    jobProps.put("propB", "b");
    jobProps.put(CommonJobProperties.JOB_CLUSTER_COMPONENTS_DEPENDENCIES, "hive");
    Job job = manager.buildJobExecutor("anothertestjob", jobProps, LOG);

    jobProps = ((FakeJavaJob) job).getJobProps();
    Assert.assertEquals(
        defaultCluster.clusterId, jobProps.getString(CommonJobProperties.TARGET_CLUSTER));

    String clusterClassPath = jobProps.getString(CommonJobProperties.TARGET_CLUSTER_CLASSPATH);
    Assert.assertTrue(clusterClassPath.contains("hive"));
    Assert.assertTrue(clusterClassPath.contains("hadoop"));

    String jvmArgs = jobProps.getString(CommonJobProperties.TARGET_CLUSTER_NATIVE_LIB);
    Assert.assertTrue(jvmArgs.contains("hadoop"));
    Assert.assertTrue(jvmArgs.contains("hive"));
  }

  /**
   * Unit test of JobTypeManager's setup for jobs that depend on only a subset of
   * components of the default cluster.
   */
  @Test
  public void testJobTypeManagerJobSetupWithoutJobtypeComponentDependency() {
    JobTypeManager manager = new JobTypeManager(this.testPluginDirPath, null,
        this.getClass().getClassLoader(), this.clusterRouter);

    Props jobProps = new Props();
    jobProps.put("type", "testjob");
    jobProps.put("propA", "a");
    jobProps.put(CommonJobProperties.JOB_CLUSTER_COMPONENTS_DEPENDENCIES, "hadoop");
    Job job = manager.buildJobExecutor("testjob", jobProps, LOG);

    jobProps = ((FakeJavaJob2) job).getJobProps();
    Assert.assertEquals(
        defaultCluster.clusterId, jobProps.getString(CommonJobProperties.TARGET_CLUSTER));

    String clusterClassPath = jobProps.getString(CommonJobProperties.TARGET_CLUSTER_CLASSPATH);
    Assert.assertFalse(clusterClassPath.contains("hive"));
    Assert.assertTrue(clusterClassPath.contains("hadoop"));

    String jvmArgs = jobProps.getString(CommonJobProperties.TARGET_CLUSTER_NATIVE_LIB);
    Assert.assertFalse(jvmArgs.contains("hive"));
    Assert.assertTrue(jvmArgs.contains("hadoop"));
  }

  /**
   * Unit test of JobTypeManager's setup for jobs that depend on only a subset of
   * components of the default cluster.
   */
  @Test (expected = JobTypeManagerException.class)
  public void testJobTypeManagerJobSetupWithUnknownClusterComponents() {
    JobTypeManager manager = new JobTypeManager(this.testPluginDirPath, null,
        this.getClass().getClassLoader(), this.clusterRouter);

    Props jobProps = new Props();
    jobProps.put("type", "anothertestjob");
    jobProps.put("propB", "b");
    jobProps.put(CommonJobProperties.JOB_CLUSTER_COMPONENTS_DEPENDENCIES, "unknown");
    manager.buildJobExecutor("anothertestjob", jobProps, LOG);
  }
}
