package azkaban.cluster;

import azkaban.utils.Props;
import com.linkedin.robin.api.ClusterSpec;
import com.linkedin.robin.api.JobSpec;
import com.linkedin.robin.client.RobinClient;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An implementation of {@link ClusterRouter} that delegates the decision to Robin.
 */
@Singleton
public class RobinBasedRouter extends ClusterRouter {
  private static final Logger LOG = LoggerFactory.getLogger(RobinBasedRouter.class);
  private final Configuration robinClientConf;

  @Inject
  public RobinBasedRouter(ClusterRegistry clusterRegistry, Configuration robinClientConf) {
    super(clusterRegistry, robinClientConf);
    this.robinClientConf = robinClientConf;
  }

  @Override
  public Cluster getCluster(String jobId, Props jobProps, Collection<String> componentDependency) {
    JobSpec jobSpec =  new JobSpec("*", jobId, getJobExecutionEnv(jobProps), Collections.emptyMap());
    RobinClient robinClient = null;
    try {
      robinClient = new RobinClient();
      robinClient.init(robinClientConf);
      robinClient.start();
      ClusterSpec clusterSpec = robinClient.getCluster(jobSpec);
      Cluster cluster = clusterRegistry.getCluster(clusterSpec.getClusterId());

      Props mergedClusterProps = new Props(cluster.properties);
      // override cluster properties loaded locally
      for (Map.Entry<String, String> entry : clusterSpec.getProperties().entrySet()) {
        mergedClusterProps.put(entry.getKey(), entry.getValue());
      }
      return new Cluster(clusterSpec.getClusterId(), mergedClusterProps);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      if (robinClient != null) {
        robinClient.stop();
      }
    }
  }

  @Override
  public Cluster getCluster(String clusterId) {
    return clusterRegistry.getCluster(clusterId);
  }

  private static Map<String, String> getJobExecutionEnv(Props jobProps) {
    List<String> jobExecutionEnvStrs = jobProps.getStringList("job.execution.env", ",");
    Map<String, String> jobExecutionEnv = new HashMap<>(jobExecutionEnvStrs.size());
    for(String jees: jobExecutionEnvStrs) {
      String[] parts = jees.split(":");
      if (parts.length != 2) {
        throw new RuntimeException("invalid job execution env pair specified: " + jees);
      }
      jobExecutionEnv.put(parts[0], parts[1]);
    }
    return jobExecutionEnv;
  }
}
