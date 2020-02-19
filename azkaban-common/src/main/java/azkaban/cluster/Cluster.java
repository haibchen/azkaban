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
import joptsimple.internal.Strings;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

public class Cluster {
  public static final Cluster UNKNOWN = new Cluster("UNKNOWN", new Props());
  public static final String DEFAULT_CLUSTER = "default";
  public static final String PATH_DELIMITER = ":";

  public static final String HADOOP_SECURITY_MANAGER_CLASS_PROP =
      "hadoop.security.manager.class";
  public static final String NATIVE_LIBRARY_PATH_PREFIX =
      "native.library.path.";
  public static final String LIBRARY_PATH_PREFIX = "library.path.";
  public static final String EXCLUDED_LIBRARY_PATTERNS = "library.excluded.patterns";

  public final String clusterId;
  public final Props properties;
  private final List<Pattern> excludedLibraryPatterns;
  private final Map<String, List<URL>> componentURLs = new ConcurrentHashMap<>();

  public Cluster(String clusterId, Props properties) {
    this.clusterId = clusterId;
    this.properties = properties;

    List<String> exclusionPatterns = properties.getStringList(EXCLUDED_LIBRARY_PATTERNS);
    this.excludedLibraryPatterns = new ArrayList<>(exclusionPatterns.size());
    for (String exclusion : exclusionPatterns) {
      excludedLibraryPatterns.add(Pattern.compile(exclusion));
    }
  }

  @Override
  public String toString() {
    return "cluster: " + clusterId + " with properties " + properties;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof Cluster)) {
      return false;
    }
    Cluster other = (Cluster) obj;

    return Objects.equals(other.clusterId, this.clusterId) &&
        Objects.equals(other.properties, this.properties) &&
        Objects.equals(excludedLibraryPatterns, other.excludedLibraryPatterns);
  }

  @Override
  public int hashCode() {
    return Objects.hash(clusterId, properties, excludedLibraryPatterns);
  }

  /**
   * Get library URLs for a given set of components.
   */
  public List<URL> getClusterComponentURLs(Collection<String> components)
      throws MalformedURLException {
    List<URL> urls = new ArrayList<>();
    Map<String, String> componentClasspath = this.properties.getMapByPrefix(
        Cluster.LIBRARY_PATH_PREFIX);
    for (String compo : components) {
      if (!componentURLs.containsKey(compo)) {
        String classpath = componentClasspath.get(compo);
        if (classpath == null) {
          throw new IllegalArgumentException("Could not find component " + compo + " from cluster: " + clusterId);
        }
        componentURLs.putIfAbsent(compo, getResourcesFromClasspath(classpath));
      }
      urls.addAll(componentURLs.get(compo));
    }
    return urls;
  }

  private List<URL> getResourcesFromClasspath(String clusterLibraryPath)
      throws MalformedURLException {
    List<URL> resources = new ArrayList<>();

    if (clusterLibraryPath == null || clusterLibraryPath.isEmpty()) {
      return resources;
    }

    for (String path : clusterLibraryPath.split(PATH_DELIMITER)) {
      File file = new File(path);
      if (file.isFile()) {
        if (!shouldExclude(file)) {
          resources.add(file.toURI().toURL());
        }
      } else {
        // strip the trailing * character from the path
        path = path.replaceAll("\\*$", "");
        File folder = new File(path);
        if (folder.exists()) {
          resources.add(folder.toURI().toURL());
          for (File jar : folder.listFiles()) {
            if (jar.getName().endsWith(".jar") && !shouldExclude(jar)) {
              resources.add(jar.toURI().toURL());
            }
          }
        }
      }
    }

    return resources;
  }

  private boolean shouldExclude(File file) {
    if (file == null) {
      return true;
    }
    String fileName = file.getName();
    for (Pattern exclude : excludedLibraryPatterns) {
      if (exclude.matcher(fileName).matches()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Get the native library path as a String to be provided through -Djava.library.path
   * to job JVM process.
   */
  public String getNativeLibraryPath(Collection<String> components) {
    return getNativeLibraryPath(properties, components);
  }

  public static String getNativeLibraryPath(Props properties, Collection<String> components) {
    List<String> nativeLibraryLibPaths = new ArrayList<>();
    Map<String, String> compoNativeLibPaths = properties.getMapByPrefix(
        Cluster.NATIVE_LIBRARY_PATH_PREFIX);
    for (String compo : components) {
      String nativeLibPath = compoNativeLibPaths.get(compo);
      if (nativeLibPath != null) {
        nativeLibraryLibPaths.add(nativeLibPath);
      }
    }
    return nativeLibraryLibPaths.isEmpty() ? Strings.EMPTY : String.join(PATH_DELIMITER, nativeLibraryLibPaths);
  }

  /**
   * Get library paths for a given set of components as a ':' separated string.
   */
  public String getJavaLibraryPath(Collection<String> components) {
    return getJavaLibraryPath(properties, components);
  }

  public static String getJavaLibraryPath(Props properties, Collection<String> components) {
    List<String> classpaths = new ArrayList<>();
    Map<String, String> compoClasspaths = properties.getMapByPrefix(
        Cluster.LIBRARY_PATH_PREFIX);
    for (String compo : components) {
      String libPath = compoClasspaths.get(compo);
      if (libPath != null) {
        classpaths.add(libPath);
      }
    }
    return classpaths.isEmpty() ? Strings.EMPTY : String.join(PATH_DELIMITER, classpaths);
  }
}
