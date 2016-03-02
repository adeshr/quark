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

package com.qubole.quark.serverclient.client;

/**
 * Utility for Quark thin clients.
 */
public final class ThinClientUtil {
  private static final String DEFAULT_SERIALIZATION = "PROTOBUF";

  private ThinClientUtil() {}

  public static String getConnectionUrl(String hostname, int port) {
    return getConnectionUrl("http", hostname, port);
  }

  public static String getConnectionUrl(String protocol, String hostname, int port) {
    return getConnectionUrl(protocol, hostname, port, DEFAULT_SERIALIZATION);
  }

  public static String getConnectionUrl(String protocol,
                                        String hostname,
                                        int port,
                                        String serialization) {
    String urlFmt = QuarkDriver.CONNECT_STRING_PREFIX + "url=%s://%s:%s;serialization=%s";
    return String.format(urlFmt, protocol, hostname, port, serialization);
  }
}
