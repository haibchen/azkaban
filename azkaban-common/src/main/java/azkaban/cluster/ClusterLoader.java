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
package azkaban.cluster;

import azkaban.utils.Props;
import azkaban.utils.PropsUtils;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads clusters and their information from a directory and add them to {@link ClusterRegistry}.
 */
@Singleton
public class ClusterLoader {
  private static final Logger LOG = LoggerFactory.getLogger(ClusterLoader.class);

  private final File clustersDir;
  private final ClusterRegistry clusterRegistry;

  @Inject
  public ClusterLoader(@Named("clusterDir") File clustersDir, ClusterRegistry clusterRegistry)
          throws IOException {
    this.clustersDir = clustersDir;
    this.clusterRegistry = clusterRegistry;

    // TODO move it out of constructor
    loadClusters();
  }

  public void loadClusters() throws IOException {
    if (clustersDir.exists()) {
      LOG.info("Loading clusters from directory " + clustersDir);
      for (final File clusterDir : clustersDir.listFiles()) {
        if (clusterDir.isDirectory() && clusterDir.canRead()) {
          LOG.info("Loading cluster from directory: " + clusterDir);
          loadCluster(clusterDir, clusterRegistry);
          LOG.info("Loaded " + clusterDir.getName() + " from " + clusterDir);
        }
      }
    }
  }

  /**
   * Load a cluster from a directory into the ClusterRegistry.
   */
  public static void loadCluster(File clusterDir, ClusterRegistry clusterRegistry)
      throws IOException {
    File clusterConfigFile = new File(clusterDir, "cluster.properties");
    if (!clusterConfigFile.exists()) {
      throw new FileNotFoundException("cluster.properties is missing under " + clusterDir);
    }

    Props clusterConfigProp = new Props(null, clusterConfigFile);
    Props resolvedClusterConfigProp = PropsUtils.resolveProps(clusterConfigProp);

    String clusterId = clusterDir.getName();
    Cluster clusterInfo = new Cluster(clusterId, resolvedClusterConfigProp);
    clusterRegistry.addCluster(clusterId, clusterInfo);
  }
}
