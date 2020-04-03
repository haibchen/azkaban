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

package azkaban.executor;

import azkaban.Constants;
import azkaban.flow.CommonJobProperties;
import azkaban.utils.TypedMapWrapper;

import java.util.HashMap;
import java.util.Map;

public class ClusterInfo {
  public final String clusterId;
  public final String clusterURL;
  public final String resourceManagerURL;
  public final String historyServerURL;
  public final String sparkHistoryServerULR;

  public ClusterInfo(String clusterId, String clusterURL, String resourceManagerURL,
                     String historyServerURL, String sparkHistoryServerULR) {
    this.clusterId = clusterId;
    this.clusterURL = clusterURL;
    this.resourceManagerURL = resourceManagerURL;
    this.historyServerURL = historyServerURL;
    this.sparkHistoryServerULR = sparkHistoryServerULR;
  }

  public static ClusterInfo fromObject(Object obj) {
    Map<String, Object> map = (Map<String, Object>) obj;
    TypedMapWrapper<String, Object> wrapper = new TypedMapWrapper<>(map);
    String clusterId = wrapper.getString(CommonJobProperties.TARGET_CLUSTER);
    String clusterURL = wrapper.getString(Constants.ConfigurationKeys.HADOOP_CLUSTER_URL);
    String rmURL = wrapper.getString(Constants.ConfigurationKeys.RESOURCE_MANAGER_JOB_URL);
    String hsURL = wrapper.getString(Constants.ConfigurationKeys.HISTORY_SERVER_JOB_URL);
    String shsURL = wrapper.getString(Constants.ConfigurationKeys.SPARK_HISTORY_SERVER_JOB_URL);
    return new ClusterInfo(clusterId, clusterURL, rmURL, hsURL, shsURL);
  }

  public static Map<String, Object> toObject(ClusterInfo cluster) {
    HashMap<String, Object> map = new HashMap<>();
    map.put(CommonJobProperties.TARGET_CLUSTER, cluster.clusterId);
    map.put(Constants.ConfigurationKeys.HADOOP_CLUSTER_URL, cluster.clusterURL);
    map.put(Constants.ConfigurationKeys.RESOURCE_MANAGER_JOB_URL, cluster.resourceManagerURL);
    map.put(Constants.ConfigurationKeys.HISTORY_SERVER_JOB_URL, cluster.historyServerURL);
    map.put(Constants.ConfigurationKeys.SPARK_HISTORY_SERVER_JOB_URL, cluster.sparkHistoryServerULR);
    return map;
  }
}
