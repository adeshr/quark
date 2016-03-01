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

#
# Script to handle launching the query server process.
#
# usage: queryserver.py [start|stop|makeWinServiceDesc] [-Dhadoop=configs]
#

import datetime
import getpass
import os
import os.path
import signal
import subprocess
import sys
import tempfile

try:
    import daemon
    daemon_supported = True
except ImportError:
    # daemon script not supported on some platforms (windows?)
    daemon_supported = False

import quark_utils

quark_utils.setPath()

command = None
args = sys.argv

if len(args) > 1:
    if args[1] == 'start':
        command = 'start'
    elif args[1] == 'stop':
        command = 'stop'
    elif args[1] == 'makeWinServiceDesc':
        command = 'makeWinServiceDesc'

if command:
    # Pull off queryserver.py and the command
    args = args[2:]
else:
    # Just pull off queryserver.py
    args = args[1:]

if os.name == 'nt':
    args = subprocess.list2cmdline(args)
else:
    import pipes    # pipes module isn't available on Windows
    args = " ".join([pipes.quote(v) for v in args])

java_home = os.getenv('JAVA_HOME')
quark_log_dir = os.path.join(tempfile.gettempdir())
quark_file_basename = 'quark-server-%s' % getpass.getuser()
quark_log_file = '%s.log' % quark_file_basename
quark_out_file = '%s.out' % quark_file_basename
quark_pid_file = '%s.pid' % quark_file_basename

log_file_path = os.path.join(quark_log_dir, quark_log_file)
out_file_path = os.path.join(quark_log_dir, quark_out_file)
pid_file_path = os.path.join(quark_pid_file)

if java_home:
    java = os.path.join(java_home, 'bin', 'java')
else:
    java = 'java'

#    " -Xdebug -Xrunjdwp:transport=dt_socket,address=5005,server=y,suspend=n " + \
#    " -XX:+UnlockCommercialFeatures -XX:+FlightRecorder -XX:FlightRecorderOptions=defaultrecording=true,dumponexit=true" + \

# The command is run through subprocess so environment variables are automatically inherited
java_cmd = '%(java)s -cp ' + \
           quark_utils.quark_server_jar + \
           " -Dproc_quarkserver" + \
           " -Dpsql.root.logger=%(root_logger)s" + \
           " -Dpsql.log.dir=%(log_dir)s" + \
           " -Dpsql.log.file=%(log_file)s" + \
           " com.qubole.quark.serverclient.server.Main " + args

if command == 'makeWinServiceDesc':
    cmd = java_cmd % {'java': java, 'root_logger': 'INFO,DRFA,console', 'log_dir': quark_log_dir, 'log_file': quark_log_file}
    slices = cmd.split(' ')

    print "<service>"
    print "  <name>Quark Server</name>"
    print "  <description>This service runs the Quark Server.</description>"
    print "  <executable>%s</executable>" % slices[0]
    print "  <arguments>%s</arguments>" % ' '.join(slices[1:])
    print "</service>"
    sys.exit()

if command == 'start':
    if not daemon_supported:
        print >> sys.stderr, "daemon mode not supported on this platform"
        sys.exit(-1)

    # run in the background
    d = os.path.dirname(out_file_path)
    if not os.path.exists(d):
        os.makedirs(d)
    with open(out_file_path, 'a+') as out:
        context = daemon.DaemonContext(
                pidfile = daemon.PidFile(pid_file_path, 'Query Server already running, PID file found: %s' % pid_file_path),
                stdout = out,
                stderr = out,
        )
        print 'starting Query Server, logging to %s' % log_file_path
        with context:
            # this block is the main() for the forked daemon process
            child = None
            cmd = java_cmd % {'java': java, 'root_logger': 'INFO', 'log_dir': quark_log_dir, 'log_file': quark_log_file}

            # notify the child when we're killed
            def handler(signum, frame):
                if child:
                    child.send_signal(signum)
                sys.exit(0)
            signal.signal(signal.SIGTERM, handler)

            print '%s launching %s' % (datetime.datetime.now(), cmd)
            child = subprocess.Popen(cmd.split())
            sys.exit(child.wait())

elif command == 'stop':
    if not daemon_supported:
        print >> sys.stderr, "daemon mode not supported on this platform"
        sys.exit(-1)

    if not os.path.exists(pid_file_path):
        print >> sys.stderr, "no Query Server to stop because PID file not found, %s" % pid_file_path
        sys.exit(0)

    if not os.path.isfile(pid_file_path):
        print >> sys.stderr, "PID path exists but is not a file! %s" % pid_file_path
        sys.exit(1)

    pid = None
    with open(pid_file_path, 'r') as p:
        pid = int(p.read())
    if not pid:
        sys.exit("cannot read PID file, %s" % pid_file_path)

    print "stopping Query Server pid %s" % pid
    with open(out_file_path, 'a+') as out:
        print >> out, "%s terminating Query Server" % datetime.datetime.now()
    os.kill(pid, signal.SIGTERM)

else:
    # run in the foreground using defaults from log4j.properties
    cmd = java_cmd % {'java': java, 'root_logger': 'INFO,console', 'log_dir': '.', 'log_file': 'psql.log'}
    # Because shell=True is not set, we don't have to alter the environment
    child = subprocess.Popen(cmd.split())
    sys.exit(child.wait())
