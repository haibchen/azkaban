package azkaban.cluster;

import azkaban.utils.Props;
import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;

import java.util.Collection;

/**
 * An implementation of {@link ClusterRouter} that routes jbs to the UNKNOWN cluster
 * so that the cluster implicitly loaded through Azkaban JVM will be used.
 */
public class DisabledClusterRouter extends ClusterRouter {
  public DisabledClusterRouter(ClusterRegistry clusterRegistry, Configuration configuration) {
    super(clusterRegistry, configuration);
  }

  @VisibleForTesting
  public DisabledClusterRouter() {
    super(new ClusterRegistry(), new Configuration());
  }

  @Override
  public Cluster getCluster(String jobId, Props jobProps, Collection<String> componentDependency) {
    return Cluster.UNKNOWN;
  }

  @Override
  public Cluster getCluster(String clusterId) {
    return Cluster.UNKNOWN;
  }
}
