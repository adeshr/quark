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

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Bridge between Phoenix and Avatica.
 */
public class QuarkMetaFactoryImpl implements QuarkMetaFactory {

  // invoked via reflection
  public QuarkMetaFactoryImpl() {
    super();
  }

  // invoked via reflection
  //public QuarkMetaFactoryImpl(Configuration conf) {
  //  super(conf);
  //}

  @Override
  public Meta create(List<String> args) {
    //Configuration conf = Preconditions.checkNotNull(getConf(), "Configuration must not be null.");
    Properties props = new Properties();
    String jsonString =
        "   {"
        + "     \"url\":\"jdbc:mysql://localhost.localdomain:3306/nezha_rstore\","
        + "     \"username\":\"root\","
        + "     \"password\":\"\","
        + "     \"encrypt_key\":\"easy\""
        + "   }";
    props.put("dbCredentials", jsonString);
    props.put("schemaFactory", "com.qubole.quark.catalog.db.SchemaFactory");
    //props.put("url", "jdbc:mysql://localhost.localdomain:3306/nezha_rstore");
    //props.put("username", "sa");
    //props.put("password", "");
    //info.putAll(conf.getValByRegex("avatica.*"));
    try {
      final String url;
      if (args.size() == 0) {
        url = "jdbc:quark:/home/dev/src/quark/server/target/test-classes/TpcdsModel.json";
      } else if (args.size() == 1) {
        url = args.get(0);
      } else {
        throw new RuntimeException(
            "0 or 1 argument expected. Received " + Arrays.toString(args.toArray()));
      }
      // TODO: what about -D configs passed in from cli? How do they get pushed down?
      return new QuarkMeta("jdbc:quark:", props);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
