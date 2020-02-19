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

package azkaban.jobExecutor;

import java.net.URL;
import java.net.URLClassLoader;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The classloader associated with a job being executed. It is set to
 * the context classloader of the thread executing the job.
 */
public class JobClassLoader extends URLClassLoader {
  private static final Logger LOG = LoggerFactory.getLogger(JobClassLoader.class);
  private final String jobId;
  private final ClassLoader parent;

  public JobClassLoader(URL[] urls, ClassLoader parent, String jobId) {
    super(urls, parent);
    this.parent = parent;
    this.jobId = jobId;
  }

  /**
   *
   * Try to load resources from this classloader's URLs. Note that this is
   * like the servlet spec, not the usual Java behaviour where we ask the
   * parent classloader to attempt to load first.
   */
  @Override
  public URL getResource(String name) {
    URL url = findResource(name);
    if (url == null && name.startsWith("/")) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Remove leading / off " + name);
      }
      url = findResource(name.substring(1));
    }

    if (url == null) {
      url = parent.getResource(name);
    }

    if (url != null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("getResource(" + name + ")=" + url + " for job " + jobId);
      }
    }

    return url;
  }

  @Override
  public Class<?> loadClass(String name) throws ClassNotFoundException {
    return this.loadClass(name, false);
  }

  /**
   *
   * Try to load class from this classloader's URLs. Note that this is like
   * servlet, not the standard behaviour where we ask the parent to attempt
   * to load first.
   */
  @Override
  protected synchronized Class<?> loadClass(String name, boolean resolve)
    throws ClassNotFoundException {

    if (LOG.isDebugEnabled()) {
      LOG.debug("Loading class: " + name + " for job " + jobId);
    }

    Class<?> c = findLoadedClass(name);
    ClassNotFoundException ex = null;

    // A Job instance is instantiated with an instance of Logger and Props loaded from the parent class
    // in JobTypeManager. We must delegate loading of them both to the parent class as such.
    if (c == null && !name.startsWith("org.apache.log4j") && !name.equals("azkaban.utils.Props")) {
      try {
        c = findClass(name);
        if (LOG.isDebugEnabled() && c != null) {
          LOG.debug("Loaded class: " + name + " " + " for job " + jobId);
        }
      } catch (ClassNotFoundException e) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(e.toString());
        }
        ex = e;
      }
    }

    // try parent
    if (c == null) {
      c = parent.loadClass(name);
      if (LOG.isDebugEnabled() && c != null) {
        LOG.debug("Loaded class from parent: " + name + " for job " + jobId);
      }
    }

    if (c == null) {
      throw ex != null ? ex : new ClassNotFoundException(name);
    }

    if (resolve) {
      resolveClass(c);
    }

    return c;
  }

  @VisibleForTesting
  void addURL(Class clazz) {
    super.addURL(clazz.getProtectionDomain().getCodeSource().getLocation());
  }
}
