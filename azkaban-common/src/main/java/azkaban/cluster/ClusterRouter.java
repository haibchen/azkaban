package azkaban.cluster;

import azkaban.utils.Props;
import org.apache.hadoop.conf.Configuration;

import java.util.Collection;

public abstract class ClusterRouter {
  protected final ClusterRegistry clusterRegistry;
  protected final Configuration configuration;

  public ClusterRouter(ClusterRegistry clusterRegistry, Configuration configuration) {
    this.clusterRegistry = clusterRegistry;
    this.configuration = configuration;
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
