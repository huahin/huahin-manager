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

import java.io.Serializable;
import java.util.Date;
import java.util.Map;

/**
 *
 */
public class Queue implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final int TYPE_JAR = 1;
    public static final int TYPE_HIVE = 2;

    private static final Map<Integer, String >TYPE_MAP = new java.util.HashMap<Integer, String>();
    static {
        TYPE_MAP.put(TYPE_JAR, "jar");
        TYPE_MAP.put(TYPE_HIVE, "hive");
    }

    private int type;
    private String id;
    private Date date;
    private String jarFileName;
    private String clazz;
    private String[] arguments;
    private String jar;
    private String script;
    private boolean run;
    private String message;

    /**
     * @return the type
     */
    public int getType() {
        return type;
    }

    /**
     * @param type the type to set
     */
    public void setType(int type) {
        this.type = type;
    }

    /**
     * @return the id
     */
    public String getId() {
        return id;
    }

    /**
     * @param id the id to set
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * @return the date
     */
    public Date getDate() {
        return date;
    }

    /**
     * @param date the date to set
     */
    public void setDate(Date date) {
        this.date = date;
    }

    /**
     * @return the jarFileName
     */
    public String getJarFileName() {
        return jarFileName;
    }

    /**
     * @param jarFileName the jarFileName to set
     */
    public void setJarFileName(String jarFileName) {
        this.jarFileName = jarFileName;
    }

    /**
     * @return the clazz
     */
    public String getClazz() {
        return clazz;
    }

    /**
     * @param clazz the clazz to set
     */
    public void setClazz(String clazz) {
        this.clazz = clazz;
    }

    /**
     * @return the arguments
     */
    public String[] getArguments() {
        return arguments;
    }

    /**
     * @param arguments the arguments to set
     */
    public void setArguments(String[] arguments) {
        this.arguments = arguments;
    }

    /**
     * @return the jar
     */
    public String getJar() {
        return jar;
    }

    /**
     * @param jar the jar to set
     */
    public void setJar(String jar) {
        this.jar = jar;
    }

    /**
     * @return the script
     */
    public String getScript() {
        return script;
    }

    /**
     * @param script the script to set
     */
    public void setScript(String script) {
        this.script = script;
    }

    /**
     * @return the run
     */
    public boolean isRun() {
        return run;
    }

    /**
     * @param run the run to set
     */
    public void setRun(boolean run) {
        this.run = run;
    }

    /**
     * @return the message
     */
    public String getMessage() {
        return message;
    }

    /**
     * @param message the message to set
     */
    public void setMessage(String message) {
        this.message = message;
    }

    /**
     * @param type
     * @return type to string
     */
    public static String toType(int type) {
        return TYPE_MAP.get(type);
    }
}
