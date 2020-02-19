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

import java.util.HashMap;
import java.util.Map;
import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class ClusterRegistry {
  private final Map<String, Cluster> clusterInfoMap;

  @Inject
  public ClusterRegistry() {
    this.clusterInfoMap = new HashMap<>();
  }

  public synchronized Cluster getCluster(String clusterId) {
    return clusterInfoMap.getOrDefault(clusterId, Cluster.UNKNOWN);
  }

  public synchronized Cluster addCluster(String clusterId, Cluster cluster) {
    return clusterInfoMap.put(clusterId, cluster);
  }

  public synchronized boolean isEmpty() {
    return this.clusterInfoMap.isEmpty();
  }
}
