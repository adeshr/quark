/*
 * Copyright (c) 2015. Qubole Inc
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.qubole.quark.serverclient.server;

import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.remote.Driver;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.remote.Service;
import org.apache.calcite.avatica.server.HandlerFactory;
import org.apache.calcite.avatica.server.HttpServer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.annotations.VisibleForTesting;

import org.eclipse.jetty.server.Handler;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * A query server for Phoenix over Calcite's Avatica.
 */
public final class Main implements Runnable {

  protected static final Log LOG = LogFactory.getLog(Main.class);

  private final String[] argv;
  private final CountDownLatch runningLatch = new CountDownLatch(1);
  private HttpServer server = null;
  private int retCode = 0;
  private Throwable t = null;

  /**
   * Log information about the currently running JVM.
   */
  public static void logJVMInfo() {
    // Print out vm stats before starting up.
    RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
    if (runtime != null) {
      LOG.info("vmName=" + runtime.getVmName() + ", vmVendor="
          + runtime.getVmVendor() + ", vmVersion=" + runtime.getVmVersion());
      LOG.info("vmInputArguments=" + runtime.getInputArguments());
    }
  }

  /**
   * Logs information about the currently running JVM process including
   * the environment variables. Logging of env vars can be disabled by
   * setting {@code "phoenix.envvars.logging.disabled"} to {@code "true"}.
   * <p>If enabled, you can also exclude environment variables containing
   * certain substrings by setting {@code "phoenix.envvars.logging.skipwords"}
   * to comma separated list of such substrings.
   */
  public static void logProcessInfo() {
    // log environment variables unless asked not to
    /*if (!conf.getBoolean("phoenix.queryserver.envvars.logging.disabled", false)) {
      Set<String> skipWords = new HashSet<String>(
      QueryServicesOptions.DEFAULT_QUERY_SERVER_SKIP_WORDS);
      if (false) {
        String[] confSkipWords = null;
        //conf.getStrings(QueryServices.QUERY_SERVER_ENV_LOGGING_SKIPWORDS_ATTRIB);
        if (confSkipWords != null) {
          skipWords.addAll(Arrays.asList(confSkipWords));
        }
      }

      nextEnv:
      for (Map.Entry<String, String> entry : System.getenv().entrySet()) {
        String key = entry.getKey().toLowerCase();
        String value = entry.getValue().toLowerCase();
        // exclude variables which may contain skip words
        for(String skipWord : skipWords) {
          if (key.contains(skipWord) || value.contains(skipWord))
            continue nextEnv;
        }
        LOG.info("env:"+entry);
      }
    }*/
    // and JVM info
    logJVMInfo();
  }

  /** Constructor for use from. */
  public Main() {
    this(null);
  }

  /** Constructor for use as . */
  public Main(String[] argv) {
    this.argv = argv;
  }

  /**
   * @return the port number this instance is bound to, or {@code -1} if the server is not running.
   */
  @VisibleForTesting
  public int getPort() {
    if (server == null) {
      return -1;
    }
    return server.getPort();
  }

  /**
   * @return the return code from running as a.
   */
  @VisibleForTesting
  public int getRetCode() {
    return retCode;
  }

  /**
   * @return the throwable from an unsuccessful run, or null otherwise.
   */
  @VisibleForTesting
  public Throwable getThrowable() {
    return t;
  }

  /** Calling thread waits until the server is running. */
  public void awaitRunning() throws InterruptedException {
    runningLatch.await();
  }

  /** Calling thread waits until the server is running. */
  public void awaitRunning(long timeout, TimeUnit unit) throws InterruptedException {
    runningLatch.await(timeout, unit);
  }

  public void run(String[] args) throws Exception {
    logProcessInfo();
    try {

      Class<? extends QuarkMetaFactory> factoryClass = QuarkMetaFactoryImpl.class;
      int port = 8765;
      LOG.debug("Listening on port " + port);
      QuarkMetaFactory factory =
          factoryClass.getDeclaredConstructor().newInstance();
      LOG.info("CREATING META");
      Meta meta = factory.create(Collections.EMPTY_LIST); //Arrays.asList(args));
      final HandlerFactory handlerFactory = new HandlerFactory();
      Service service = new LocalService(meta);
      server = new HttpServer(port, getHandler(service, handlerFactory));
      Class.forName("com.qubole.quark.jdbc.QuarkDriver");
      server.start();
      runningLatch.countDown();
      server.join();
      System.exit(0);
    } catch (Throwable t) {
      LOG.fatal("Unrecoverable service error. Shutting down.", t);
      this.t = t;
    }
  }

  /**
   * Instantiates the Handler for use by the Avatica (Jetty) server.
   *
   * @param service The Avatica Service implementation
   * @param handlerFactory Factory used for creating a Handler
   * @return The Handler to use based on the configuration.
   */
  Handler getHandler(Service service, HandlerFactory handlerFactory) {
    String serializationName = "PROTOBUF";
    Driver.Serialization serialization;
    // Otherwise, use what was provided in the configuration
    try {
      serialization = Driver.Serialization.valueOf(serializationName);
    } catch (Exception e) {
      LOG.error("Unknown message serialization type for " + serializationName);
      throw e;
    }

    Handler handler = handlerFactory.getHandler(service, serialization);

    LOG.info("Instantiated " + handler.getClass() + " for QueryServer");

    return handler;
  }

  @Override
  public void run() {
    try {
      run(argv);
    } catch (Exception e) {
      // already logged
    }
  }

  public static void main(String[] argv) throws Exception {
    new Main().run(argv);
    System.exit(0);
  }
}
