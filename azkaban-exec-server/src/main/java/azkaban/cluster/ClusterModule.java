package azkaban.cluster;

import azkaban.execapp.AzkabanExecutorServer;
import azkaban.utils.Props;
import azkaban.utils.Utils;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import org.apache.hadoop.conf.Configuration;

import java.io.File;

public class ClusterModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(ClusterLoader.class).asEagerSingleton();
    bind(ClusterRegistry.class);
  }

  @Provides
  @Named("clusterDir")
  public File getClusterDir(final Props props) {
    String clusterDir = props.getString(AzkabanExecutorServer.CLUSTER_CONFIG_DIR, "cluster");
    return new File(clusterDir);
  }

  @Provides
  public ClusterRouter getClusterRouter(final Props props, ClusterRegistry clusterRegistry, Configuration conf)
      throws ClassNotFoundException {
    String routerClass = props.getString(AzkabanExecutorServer.CLUSTER_ROUTER_CLASS,
        DisabledClusterRouter.class.getName());
    Class<?> routerClazz = Class.forName(routerClass);
    return (ClusterRouter) Utils.callConstructor(routerClazz, clusterRegistry, conf);
  }

  @Provides
  @Singleton
  public Configuration getRouterConf(Props props) {
    // TODO: put necessary core-site properties into robin-site.xml and do not load default xmls
    Configuration configuration = new Configuration();
    String routerConfPath = props.getString(AzkabanExecutorServer.CLUSTER_ROUTER_CONF,
        "robin-site.xml");
    configuration.addResource(routerConfPath);
    return configuration;
  }
}
