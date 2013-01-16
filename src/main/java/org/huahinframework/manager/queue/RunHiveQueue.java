/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.huahinframework.manager.queue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 */
public class RunHiveQueue extends Thread {
    private static final Log log = LogFactory.getLog(RunHiveQueue.class);

    private static final String DRIVER_NAME = "org.apache.hadoop.hive.jdbc.HiveDriver";
    private static final String CONNECTION_FORMAT = "jdbc:hive://%s/default";

    private String hiveserver;
    private String queuePath;
    private Queue queue;

    /**
     * @param queue
     */
    public RunHiveQueue(String hiveserver, String queuePath, Queue queue) {
        this.hiveserver = hiveserver;
        this.queuePath = queuePath;
        this.queue = queue;
    }

    /* (non-Javadoc)
     * @see java.lang.Thread#run()
     */
    @Override
    public void run() {
        try {
            Class.forName(DRIVER_NAME);
            Connection con = DriverManager.getConnection(String.format(CONNECTION_FORMAT, hiveserver), "", "");
            Statement stmt = con.createStatement();
            stmt.executeQuery(queue.getScript());
        } catch (Exception e) {
            queue.setMessage(e.toString());
            try {
                QueueUtils.registerQueue(queuePath, queue);
            } catch (IOException e1) {
                e1.printStackTrace();
                log.error(e1);
            }
            e.printStackTrace();
            log.error(e);
        }
    }
}
