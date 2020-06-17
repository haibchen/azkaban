/*
 * Copyright 2012 LinkedIn Corp.
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

import azkaban.Constants;
import azkaban.cluster.Cluster;
import azkaban.cluster.ClusterRouter;
import azkaban.cluster.DisabledClusterRouter;
import azkaban.flow.CommonJobProperties;
import azkaban.jobExecutor.JavaProcessJob;
import azkaban.jobExecutor.Job;
import azkaban.jobExecutor.JobClassLoader;
import azkaban.jobExecutor.NoopJob;
import azkaban.jobExecutor.ProcessJob;
import azkaban.jobExecutor.utils.JobExecutionException;
import azkaban.utils.Props;
import azkaban.utils.PropsUtils;
import azkaban.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import org.apache.log4j.Logger;

public class JobTypeManager {
  private static final Logger LOGGER = Logger.getLogger(JobTypeManager.class);

  private final String jobTypePluginDir; // the dir for jobtype plugins
  private final ClassLoader parentLoader;
  private final Props globalProperties;
  private final ClusterRouter clusterRouter;
  private JobTypePluginSet pluginSet;

  @VisibleForTesting
  public JobTypeManager(final String jobtypePluginDir, final Props globalProperties,
      final ClassLoader parentClassLoader) {
    this(jobtypePluginDir, globalProperties, parentClassLoader, new DisabledClusterRouter());
  }

  public JobTypeManager(final String jobtypePluginDir, final Props globalProperties,
    final ClassLoader parentClassLoader, ClusterRouter clusterRouter) {
    this.jobTypePluginDir = jobtypePluginDir;
    this.parentLoader = parentClassLoader;
    this.globalProperties = globalProperties;
    this.clusterRouter = clusterRouter;
    loadPlugins();
  }

  public void loadPlugins() throws JobTypeManagerException {
    final JobTypePluginSet plugins = new JobTypePluginSet();

    loadDefaultTypes(plugins);
    if (this.jobTypePluginDir != null) {
      final File pluginDir = new File(this.jobTypePluginDir);
      if (pluginDir.exists()) {
        LOGGER.info("Job type plugin directory set. Loading extra job types from " + pluginDir);
        try {
          loadPluginJobTypes(plugins);
        } catch (final Exception e) {
          LOGGER.info("Plugin jobtypes failed to load. " + e.getCause(), e);
          throw new JobTypeManagerException(e);
        }
      }
    }

    // Swap the plugin set. If exception is thrown, then plugin isn't swapped.
    synchronized (this) {
      this.pluginSet = plugins;
    }
  }

  private void loadDefaultTypes(final JobTypePluginSet plugins)
      throws JobTypeManagerException {
    LOGGER.info("Loading plugin default job types");
    plugins.addPluginClassName("command", ProcessJob.class.getName());
    plugins.addPluginClassName("javaprocess", JavaProcessJob.class.getName());
    plugins.addPluginClassName("noop", NoopJob.class.getName());
  }

  // load Job Types from jobtype plugin dir
  private void loadPluginJobTypes(final JobTypePluginSet plugins)
      throws JobTypeManagerException {
    final File jobPluginsDir = new File(this.jobTypePluginDir);

    if (!jobPluginsDir.exists()) {
      LOGGER.error("Job type plugin dir " + this.jobTypePluginDir
          + " doesn't exist. Will not load any external plugins.");
      return;
    } else if (!jobPluginsDir.isDirectory()) {
      throw new JobTypeManagerException("Job type plugin dir "
          + this.jobTypePluginDir + " is not a directory!");
    } else if (!jobPluginsDir.canRead()) {
      throw new JobTypeManagerException("Job type plugin dir "
          + this.jobTypePluginDir + " is not readable!");
    }

    // Load the common properties used by all jobs that are run
    Props commonPluginJobProps = null;
    final File commonJobPropsFile = new File(jobPluginsDir, Constants.PluginManager.COMMONCONFFILE);
    if (commonJobPropsFile.exists()) {
      LOGGER.info("Common plugin job props file " + commonJobPropsFile
          + " found. Attempt to load.");
      try {
        commonPluginJobProps = new Props(this.globalProperties, commonJobPropsFile);
      } catch (final IOException e) {
        throw new JobTypeManagerException(
            "Failed to load common plugin job properties" + e.getCause());
      }
    } else {
      LOGGER.info("Common plugin job props file " + commonJobPropsFile
          + " not found. Using only globals props");
      commonPluginJobProps = new Props(this.globalProperties);
    }

    // Loads the common properties used by all plugins when loading
    Props commonPluginLoadProps = null;
    final File commonLoadPropsFile = new File(jobPluginsDir, Constants.PluginManager.COMMONSYSCONFFILE);
    if (commonLoadPropsFile.exists()) {
      LOGGER.info("Common plugin load props file " + commonLoadPropsFile
          + " found. Attempt to load.");
      try {
        commonPluginLoadProps = new Props(null, commonLoadPropsFile);
      } catch (final IOException e) {
        throw new JobTypeManagerException(
            "Failed to load common plugin loader properties" + e.getCause());
      }
    } else {
      LOGGER.info("Common plugin load props file " + commonLoadPropsFile
          + " not found. Using empty props.");
      commonPluginLoadProps = new Props();
    }

    plugins.setCommonPluginJobProps(commonPluginJobProps);
    plugins.setCommonPluginLoadProps(commonPluginLoadProps);

    // Loading job types
    for (final File dir : jobPluginsDir.listFiles()) {
      if (dir.isDirectory() && dir.canRead()) {
        try {
          loadJobTypes(dir, plugins);
        } catch (final Exception e) {
          LOGGER.error("Failed to load jobtype " + dir.getName() + e.getMessage(), e);
          throw new JobTypeManagerException(e);
        }
      }
    }
  }

  private void loadJobTypes(final File pluginDir, final JobTypePluginSet plugins)
      throws JobTypeManagerException {
    // Directory is the jobtypeName
    final String jobTypeName = pluginDir.getName();
    LOGGER.info("Loading plugin " + jobTypeName);

    Props pluginJobProps = null;
    Props pluginLoadProps = null;
    Props pluginPrivateProps = null;

    final File pluginJobPropsFile = new File(pluginDir, Constants.PluginManager.CONFFILE);
    final File pluginLoadPropsFile = new File(pluginDir, Constants.PluginManager.SYSCONFFILE);

    if (!pluginLoadPropsFile.exists()) {
      LOGGER.info("Plugin load props file " + pluginLoadPropsFile + " not found.");
      return;
    }

    try {
      final Props commonPluginJobProps = plugins.getCommonPluginJobProps();
      final Props commonPluginLoadProps = plugins.getCommonPluginLoadProps();
      if (pluginJobPropsFile.exists()) {
        pluginJobProps = new Props(commonPluginJobProps, pluginJobPropsFile);
      } else {
        pluginJobProps = new Props(commonPluginJobProps);
      }

      // Set the private props.
      pluginPrivateProps = new Props(null, pluginLoadPropsFile);
      pluginPrivateProps.put("plugin.dir", pluginDir.getAbsolutePath());
      plugins.addPluginPrivateProps(jobTypeName, pluginPrivateProps);

      pluginLoadProps = new Props(commonPluginLoadProps, pluginPrivateProps);

      // Adding "plugin.dir" to allow plugin.properties file could read this property. Also, user
      // code could leverage this property as well.
      pluginJobProps.put("plugin.dir", pluginDir.getAbsolutePath());
      pluginLoadProps = PropsUtils.resolveProps(pluginLoadProps);
    } catch (final Exception e) {
      LOGGER.error("pluginLoadProps to help with debugging: " + pluginLoadProps);
      throw new JobTypeManagerException("Failed to get jobtype properties"
          + e.getMessage(), e);
    }
    // Add properties into the plugin set
    plugins.addPluginLoadProps(jobTypeName, pluginLoadProps);
    if (pluginJobProps != null) {
      plugins.addPluginJobProps(jobTypeName, pluginJobProps);
    }

    URL[] urls = loadJobTypeClassLoaderURLs(pluginDir, jobTypeName, plugins);
    final ClassLoader jobTypeLoader = new URLClassLoader(urls, parentLoader);

    final String jobtypeClass = pluginLoadProps.get("jobtype.class");
    if (jobtypeClass == null) {
      throw new JobTypeManagerException("Failed to get jobtype property: jobtype.class");
    }

    String jobPropsProcessorClass = pluginLoadProps.get("jobtype.jobpropprocessor.class");
    if (jobPropsProcessorClass != null && !jobPropsProcessorClass.isEmpty()) {
      Class<? extends JobPropsProcessor> processorClazz;
      try {
        processorClazz = (Class<? extends JobPropsProcessor>) jobTypeLoader.loadClass(jobPropsProcessorClass);
        JobPropsProcessor jobPropsProcessor = (JobPropsProcessor)
            Utils.callConstructor(processorClazz, pluginLoadProps);
        plugins.addPluginJobPropsProcessor(jobTypeName, jobPropsProcessor);
      } catch (final ClassNotFoundException e) {
        throw new JobTypeManagerException(e);
      }
    }

    plugins.addPluginClassName(jobTypeName, jobtypeClass);
    plugins.addPluginClassLoaderURLs(jobTypeName, urls);
    LOGGER.info("Loaded jobtype " + jobTypeName + " " + jobtypeClass);
  }

  /**
   * Creates and loads all plugin resources (jars) into a ClassLoader
   */
  private URL[] loadJobTypeClassLoaderURLs(final File pluginDir,
      final String jobTypeName, final JobTypePluginSet plugins) {
    // sysconf says what jars/confs to load
    final List<URL> resources = new ArrayList<>();
    final Props pluginLoadProps = plugins.getPluginLoaderProps(jobTypeName);

    try {
      // first global classpath
      LOGGER.info("Adding global resources for " + jobTypeName);
      final List<String> typeGlobalClassPath =
          pluginLoadProps.getStringList("jobtype.global.classpath", null, ",");
      if (typeGlobalClassPath != null) {
        for (final String jar : typeGlobalClassPath) {
          final URL cpItem = new File(jar).toURI().toURL();
          if (!resources.contains(cpItem)) {
            LOGGER.info("adding to classpath " + cpItem);
            resources.add(cpItem);
          }
        }
      }

      // type specific classpath
      LOGGER.info("Adding type resources.");
      final List<String> typeClassPath =
          pluginLoadProps.getStringList("jobtype.classpath", null, ",");
      if (typeClassPath != null) {
        for (final String jar : typeClassPath) {
          final URL cpItem = new File(jar).toURI().toURL();
          if (!resources.contains(cpItem)) {
            LOGGER.info("adding to classpath " + cpItem);
            resources.add(cpItem);
          }
        }
      }
      final List<String> jobtypeLibDirs =
          pluginLoadProps.getStringList("jobtype.lib.dir", null, ",");
      if (jobtypeLibDirs != null) {
        for (final String libDir : jobtypeLibDirs) {
          for (final File f : new File(libDir).listFiles()) {
            if (f.getName().endsWith(".jar")) {
              resources.add(f.toURI().toURL());
              LOGGER.info("adding to classpath " + f.toURI().toURL());
            }
          }
        }
      }

      LOGGER.info("Adding type override resources.");
      for (final File f : pluginDir.listFiles()) {
        if (f.getName().endsWith(".jar")) {
          resources.add(f.toURI().toURL());
          LOGGER.info("adding to classpath " + f.toURI().toURL());
        }
      }

    } catch (final MalformedURLException e) {
      throw new JobTypeManagerException(e);
    }

    // each job type can have a different class loader
    LOGGER.info(String.format("Classpath for plugin[dir: %s, JobType: %s]: %s", pluginDir, jobTypeName,
            resources));
    return resources.toArray(new URL[resources.size()]);
  }

  @VisibleForTesting
  public Job buildJobExecutor(final String jobId, Props jobProps, final Logger logger)
      throws JobTypeManagerException {
    JobParams jobParams = createJobParams(jobId, jobProps, logger);
    return createJob(jobId, jobParams, logger);
  }

  public JobParams createJobParams(String jobId, Props jobProps, Logger logger) {
    // This is final because during build phase, you should never need to swap
    // the pluginSet for safety reasons
    final JobTypePluginSet pluginSet = getJobTypePluginSet();

    try {
      final String jobType = jobProps.getString("type");
      if (jobType == null || jobType.length() == 0) {
        /* throw an exception when job name is null or empty */
        throw new JobExecutionException(String.format(
            "The 'type' parameter for job[%s] is null or empty", jobProps));
      }

      logger.info("Building " + jobType + " job executor. ");

      final Class<?> executorClass = getJobExecutorClass(jobId, jobType, jobProps, pluginSet, logger);

      jobProps = getJobProps(jobProps, pluginSet, jobType);
      Props pluginLoadProps = getPluginLoadProps(pluginSet, jobType);

      // inject cluster jars and native libraries into jobs through properties
      jobProps.putAll(
          getClusterSpecificJobProps(jobProps, pluginLoadProps));
      jobProps = PropsUtils.resolveProps(jobProps);

      return new JobParams(executorClass, jobProps, pluginSet.getPluginPrivateProps(jobType),
          pluginLoadProps);
    } catch (final Exception e) {
      logger.error("Failed to build job executor for job " + jobId
          + e.getMessage());
      throw new JobTypeManagerException("Failed to build job executor for job "
          + jobId, e);
    } catch (final Throwable t) {
      logger.error(
          "Failed to build job executor for job " + jobId + t.getMessage(), t);
      throw new JobTypeManagerException("Failed to build job executor for job "
          + jobId, t);
    }
  }

  private Class<?> getJobExecutorClass(String jobId, String jobType, Props jobProps, JobTypePluginSet pluginSet, Logger logger)
      throws ClassNotFoundException, MalformedURLException {
    ClassLoader jobClassLoader = createJobClassLoader(jobId, jobType, jobProps, pluginSet, logger);
    String executorClassName = pluginSet.getPluginClassName(jobType);
    final Class<? extends Object> executorClass = jobClassLoader.loadClass(executorClassName);
    if (executorClass == null) {
      throw new JobExecutionException(String.format("Job type '" + jobType
              + "' is unrecognized. Could not construct job[%s] of type[%s].",
          jobProps, jobType));
    }
    return executorClass;
  }

  public ClassLoader createJobClassLoader(String jobId, String jobType, Props jobProps, JobTypePluginSet pluginSet, Logger logger)
      throws MalformedURLException {
    List<URL> urls = new ArrayList<>();

    // add jobtype declared libraries
    URL[] jobTypeURLs = pluginSet.getPluginClassLoaderURLs(jobType);
    for (URL jobTypeDependency: jobTypeURLs) {
      urls.add(jobTypeDependency);
    }

    // add cluster-specific libraries
    List<URL> clusterLibraries = getClusterSpecificURLs(jobId, jobType, jobProps, pluginSet, logger);
    urls.addAll(clusterLibraries);

    return new JobClassLoader(
        urls.toArray(new URL[urls.size()]), JobTypeManager.class.getClassLoader(), jobId);
  }

  private static Props getJobProps(Props jobProps, JobTypePluginSet pluginSet, String jobType) {
    Props pluginJobProps = pluginSet.getPluginJobProps(jobType);
    // For default jobtypes, even though they don't have pluginJobProps configured,
    // they still need to load properties from common.properties file if it's present
    // because common.properties file is global to all jobtypes.
    if (pluginJobProps == null) {
      pluginJobProps = pluginSet.getCommonPluginJobProps();
    }
    if (pluginJobProps != null) {
      for (final String k : pluginJobProps.getKeySet()) {
        if (!jobProps.containsKey(k)) {
          jobProps.put(k, pluginJobProps.get(k));
        }
      }
    }

    JobPropsProcessor propsProcessor = pluginSet.getPluginJobPropsProcessor(jobType);
    if (propsProcessor != null) {
      jobProps = propsProcessor.process(jobProps);
    }
    return jobProps;
  }

  private static Props getPluginLoadProps(JobTypePluginSet pluginSet, String jobType) {
    Props pluginLoadProps = pluginSet.getPluginLoaderProps(jobType);
    if (pluginLoadProps != null) {
      pluginLoadProps = PropsUtils.resolveProps(pluginLoadProps);
    } else {
      // pluginSet.getCommonPluginLoadProps() will return null if there is no plugins directory.
      // hence assigning default Props() if that's the case
      pluginLoadProps = pluginSet.getCommonPluginLoadProps();
      if (pluginLoadProps == null) {
        pluginLoadProps = new Props();
      }
    }
    return pluginLoadProps;
  }

  public static Job createJob(String jobId, JobParams jobParams, Logger logger) {
    Job job;
    try {
      job =
          (Job) Utils.callConstructor(jobParams.jobClass, jobId, jobParams.pluginLoadProps,
              jobParams.jobProps, jobParams.pluginPrivateProps, logger);
    } catch (final Exception e) {
      logger.info("Failed with 5 inputs with exception e = "
          + e.getMessage());
      job =
          (Job) Utils.callConstructor(jobParams.jobClass, jobId, jobParams.pluginLoadProps,
              jobParams.jobProps, logger);
    }
    return job;
  }

  public static final class JobParams {
    public final Class<? extends Object> jobClass;
    public final ClassLoader jobClassLoader;
    public final Props jobProps;
    public final Props pluginLoadProps;
    public final Props pluginPrivateProps;

    JobParams(Class<? extends Object> jobClass, Props jobProps,
                     Props pluginPrivateProps, Props pluginLoadProps) {
      this.jobClass = jobClass;
      this.jobClassLoader = jobClass.getClassLoader();
      this.jobProps = jobProps;
      this.pluginLoadProps = pluginLoadProps;
      this.pluginPrivateProps = pluginPrivateProps;
    }
  }

  /**
   * Public for test reasons. Will need to move tests to the same package
   */
  public synchronized JobTypePluginSet getJobTypePluginSet() {
    return this.pluginSet;
  }

  /**
   * Add cluster-specific libraries/jars. if no such cluster is configured, we assume
   * cluster-specific URLs will be available through the parent classloader from AZ.
   */
  private List<URL> getClusterSpecificURLs(String jobId, String jobType, Props jobProps, JobTypePluginSet pluginSet, Logger logger)
      throws MalformedURLException {
    Collection<String> components =
        getClusterComponents(jobProps, pluginSet.getPluginLoaderProps(jobType));

    if (components.isEmpty()) {
      // This job is local with no dependency on a remote cluster, skip routing
      return Collections.emptyList();
    }
    Cluster targetCluster = this.clusterRouter.getCluster(jobId, jobProps, components);

    if (targetCluster != null && !Cluster.UNKNOWN.equals(targetCluster)) {
      logger.info("Routing " + jobId + " to cluster: " + targetCluster.clusterId);
      // inject the cluster id to as a job property
      jobProps.put(CommonJobProperties.TARGET_CLUSTER, targetCluster.clusterId);
      return targetCluster.getClusterComponentURLs(components);
    }

    return Collections.emptyList();
  }

  /**
   * Expose cluster-specific libraries and native libraries through job properties.
   * if a router is configured, construct the properties based on cluster.properties
   * otherwise, the cluster is implicitly configured, the properties will be based
   * on plugins' private properties.
   */
  private Props getClusterSpecificJobProps(Props jobProps, Props pluginProps) {
    Props clusterProps = new Props();

    Props sourceProps;

    String clusterId = jobProps.get(CommonJobProperties.TARGET_CLUSTER);
    if (clusterId != null && !Cluster.UNKNOWN.equals(clusterRouter.getCluster(clusterId))){
      sourceProps = clusterRouter.getCluster(clusterId).properties;
      clusterProps.putAll(sourceProps);
    } else {
      // fall back to the existing mechanism if no cluster is found/configured
      sourceProps = pluginProps;
    }

    Collection<String> components = getClusterComponents(jobProps, pluginProps);

    String javaLibPath = Cluster.getJavaLibraryPath(sourceProps, components);
    if (javaLibPath != null && !javaLibPath.isEmpty()) {
      clusterProps.put(CommonJobProperties.TARGET_CLUSTER_CLASSPATH, javaLibPath);
    }
    String nativeLibPath = Cluster.getNativeLibraryPath(sourceProps, components);
    if (nativeLibPath != null && !nativeLibPath.isEmpty()) {
      clusterProps.put(CommonJobProperties.TARGET_CLUSTER_NATIVE_LIB, nativeLibPath);
    }
    String hadoopSecurityManagerClazz =
        sourceProps.get(Cluster.HADOOP_SECURITY_MANAGER_CLASS_PROP);
    if (hadoopSecurityManagerClazz != null) {
      clusterProps.put(
          Cluster.HADOOP_SECURITY_MANAGER_CLASS_PROP, hadoopSecurityManagerClazz);
    }

    return clusterProps;
  }

  /**
   * Get the components within a cluster that a job depends on.
   */
  private static Collection<String> getClusterComponents(Props jobProps, Props pluginProps) {
    // use ordered set to maintain the classpath order as much as possible
    Set<String> components = new LinkedHashSet<>(4);

    if (pluginProps != null) {
      List<String> jobtypeComponents = pluginProps.getStringList(
          CommonJobProperties.JOBTYPE_CLUSTER_COMPONENTS_DEPENDENCIES, Collections.emptyList(), ",");
      components.addAll(jobtypeComponents);
    }
    if (jobProps != null) {
      List<String> jobTypeComponents = jobProps.getStringList(
          CommonJobProperties.JOB_CLUSTER_COMPONENTS_DEPENDENCIES, Collections.emptyList(), ",");
      components.addAll(jobTypeComponents);
    }
    return components;
  }
}
