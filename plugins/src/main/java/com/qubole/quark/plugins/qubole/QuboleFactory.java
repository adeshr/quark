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

package com.qubole.quark.plugins.qubole;

import org.apache.commons.lang.Validate;

import com.qubole.quark.QuarkException;
import com.qubole.quark.plugin.DataSource;
import com.qubole.quark.plugin.DataSourceFactory;

import java.util.Map;

/**
 * Created by dev on 11/13/15.
 */
public class QuboleFactory implements DataSourceFactory {
  @Override
  public DataSource create(Map<String, Object> properties) throws QuarkException {

    Validate.notNull(properties.get("type"),
        "Field \"type\" specifying either HIVE or DBTAP needs "
            + "to be defined for Qubole Data Source in JSON");
    Validate.notNull(properties.get("endpoint"),
        "Field \"endpoint\" specifying Qubole's endpoint needs "
        + "to be defined for Qubole Data Source in JSON");
    Validate.notNull(properties.get("token"),
        "Field \"token\" specifying Authentication token needs "
        + "to be defined for Qubole Data Source in JSON");

    String type = properties.get("type").toString();
    String token = properties.get("token").toString();
    String endpoint =  properties.get("endpoint").toString();

    if (type.toUpperCase().equals("HIVE")) {
      return new HiveDb(endpoint, token);
    } else if (type.toUpperCase().equals("DBTAP")) {
      Validate.notNull(properties.get("dbtapid"),
          "Field \"dbtapid\" needs to be defined for Qubole Data Source in JSON");
      return new DbTapDb(endpoint, token, Integer.parseInt(properties.get("dbtapid").toString()));
    } else {
      throw new QuarkException(new Throwable("Invalid qubole DataSource type:" + type
          + "\nCurrently supporting either HIVE or DBTAP"));
    }
  }
}
