package azkaban.cluster;

import azkaban.utils.Props;

import java.util.Collection;

public abstract class ClusterRouter {
  protected final ClusterRegistry clusterRegistry;

  public ClusterRouter(ClusterRegistry clusterRegistry) {
    this.clusterRegistry = clusterRegistry;
  }
  /**
   * Gets the information of the cluster that a job should be submitted to.
   */
  public abstract Cluster getCluster(String jobId, Props jobProps, Collection<String> componentDependency);

  /**
   * Get the information of a cluster give its id.
   */
  public abstract Cluster getCluster(String clusterId);
}
