#!/usr/bin/env python
############################################################################
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
############################################################################

import os
import fnmatch

def find(pattern, classPaths):
    paths = classPaths.split(os.pathsep)

    # for each class path
    for path in paths:
        # remove * if it's at the end of path
        if ((path is not None) and (len(path) > 0) and (path[-1] == '*')) :
            path = path[:-1]

        for root, dirs, files in os.walk(path):
            # sort the file names so *-client always precedes *-thin-client
            files.sort()
            for name in files:
                if fnmatch.fnmatch(name, pattern):
                    return os.path.join(root, name)

    return ""

def setPath():
    QUARK_CLIENT_JAR_PATTERN = "quark-server-client-*.jar"
    QUARK_SERVER_JAR_PATTERN = "quark-server-*.jar"

    global current_dir
    current_dir = os.path.dirname(os.path.abspath(__file__))

    global quark_client_jar
    quark_client_jar = find(QUARK_CLIENT_JAR_PATTERN, os.path.join(current_dir, "..", "client", "target", "*"))

    global quark_server_jar
    quark_server_jar = find(QUARK_SERVER_JAR_PATTERN, os.path.join(current_dir, "..", "server", "target", "*"))

    return ""

if __name__ == "__main__":
    setPath()
    print "current_dir:", current_dir
    print "client_jar:", quark_client_jar
    print "queryserver_jar:", quark_server_jar
