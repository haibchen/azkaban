package azkaban.cluster;

import azkaban.utils.Props;
import org.apache.hadoop.conf.Configuration;

import java.util.Collection;

public class RobinRouterWithFallback extends ClusterRouter {
  private final RobinBasedRouter robinRouter;
  private final DefaultClusterRouter defaultRouter;

  public RobinRouterWithFallback(ClusterRegistry clusterRegistry, Configuration configuration) {
    super(clusterRegistry, configuration);
    this.robinRouter = new RobinBasedRouter(clusterRegistry, configuration);
    this.defaultRouter = new DefaultClusterRouter(clusterRegistry, configuration);
  }

  @Override
  public Cluster getCluster(String jobId, Props jobProps, Collection<String> componentDependency) {
    try {
      return robinRouter.getCluster(jobId, jobProps, componentDependency);
    } catch (Exception e) {
      return defaultRouter.getCluster(Cluster.DEFAULT_CLUSTER);
    }
  }

  @Override
  public Cluster getCluster(String clusterId) {
    return clusterRegistry.getCluster(clusterId);
  }
}
