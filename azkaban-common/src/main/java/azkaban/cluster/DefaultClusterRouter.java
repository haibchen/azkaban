package azkaban.cluster;

import azkaban.utils.Props;
import org.apache.hadoop.conf.Configuration;

import javax.inject.Inject;
import java.util.Collection;

/**
 * An implementation of {@link ClusterRouter} that always routes jobs to the default cluster.
 */
public class DefaultClusterRouter extends ClusterRouter {
  @Inject
  public DefaultClusterRouter(ClusterRegistry clusterRegistry, Configuration configuration) {
    super(clusterRegistry, configuration);
  }

  @Override
  public Cluster getCluster(String jobId, Props jobProps, Collection<String> componentDependency) {
    String clusterId = Cluster.DEFAULT_CLUSTER;
    return this.clusterRegistry.getCluster(clusterId);
  }

  @Override
  public Cluster getCluster(String clusterId) {
    return this.clusterRegistry.getCluster(clusterId);
  }
}
