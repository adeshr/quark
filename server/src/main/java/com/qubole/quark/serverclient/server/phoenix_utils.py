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
import subprocess
import sys

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

def findFileInPathWithoutRecursion(pattern, path):
    if not os.path.exists(path):
        return ""
    files = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path,f))]
    # sort the file names so *-client always precedes *-thin-client
    files.sort()
    for name in files:
        if fnmatch.fnmatch(name, pattern):
            return os.path.join(path, name)

    return ""

def which(command):
    for path in os.environ["PATH"].split(os.pathsep):
        if os.path.exists(os.path.join(path, command)):
            return os.path.join(path, command)
    return None

def findClasspath(command_name):
    command_path = which(command_name)
    if command_path is None:
        # We don't have this command, so we can't get its classpath
        return ''
    command = "%s%s" %(command_path, ' classpath')
    return subprocess.Popen(command, shell=True, stdout=subprocess.PIPE).stdout.read()

def setPath():
    PHOENIX_CLIENT_JAR_PATTERN = "quark-server-client-*.jar"
    PHOENIX_QUERYSERVER_JAR_PATTERN = "quark-server-4.2.0.jar"

    # Backward support old env variable PHOENIX_LIB_DIR replaced by PHOENIX_CLASS_PATH
    global phoenix_class_path
    phoenix_class_path = os.getenv('PHOENIX_LIB_DIR','')
    if phoenix_class_path == "":
        phoenix_class_path = os.getenv('PHOENIX_CLASS_PATH','')

    global current_dir
    current_dir = os.path.dirname(os.path.abspath(__file__))

    global phoenix_jar_path
    phoenix_jar_path = os.path.join(current_dir, "..", "phoenix-assembly", "target","*")

    global phoenix_client_jar
    phoenix_client_jar = "/home/dev/src/quark/server/target/quark-server-client-4.2.0.jar"
        #find("", phoenix_jar_path)
    if phoenix_client_jar == "":
        phoenix_client_jar = findFileInPathWithoutRecursion(PHOENIX_CLIENT_JAR_PATTERN, os.path.join(current_dir, ".."))
    if phoenix_client_jar == "":
        phoenix_client_jar = find(PHOENIX_CLIENT_JAR_PATTERN, phoenix_class_path)

    global phoenix_queryserver_jar
    phoenix_queryserver_jar = "/home/dev/src/quark/server/target/quark-server-4.2.0.jar"
        #find(PHOENIX_QUERYSERVER_JAR_PATTERN, os.path.join(current_dir, "target", "*"))

    if phoenix_queryserver_jar == "":
        phoenix_queryserver_jar = findFileInPathWithoutRecursion(PHOENIX_QUERYSERVER_JAR_PATTERN, os.path.join(current_dir, "..", "lib"))
    if phoenix_queryserver_jar == "":
        phoenix_queryserver_jar = findFileInPathWithoutRecursion(PHOENIX_QUERYSERVER_JAR_PATTERN, os.path.join(current_dir, "target"))

    return ""

def shell_quote(args):
    """
    Return the platform specific shell quoted string. Handles Windows and *nix platforms.

    :param args: array of shell arguments
    :return: shell quoted string
    """
    if os.name == 'nt':
        import subprocess
        return subprocess.list2cmdline(args)
    else:
        # pipes module isn't available on Windows
        import pipes
        return " ".join([pipes.quote(v) for v in args])

if __name__ == "__main__":
    setPath()
    print "phoenix_class_path:", phoenix_class_path
    print "current_dir:", current_dir
    print "phoenix_jar_path:", phoenix_jar_path
    print "client_jar:", phoenix_client_jar
    print "queryserver_jar:", phoenix_queryserver_jar
