/*
 * Copyright 2024 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.connect.common.source.task;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.common.config.ConfigException;

import org.codehaus.plexus.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PartitionTaskDistribution will determine which files to process for each task, based on the partition number defined
 * in the filename e.g. the default object names created by the sink connector ex.
 * topicname-{{partition}}-{{startoffset}} the partition can be extracted to have one task running per partition.
 *
 */
public class FilenamePartitionTaskAssignment implements TaskAssignment {
    private final static Logger LOG = LoggerFactory.getLogger(FilenamePartitionTaskAssignment.class);
    private final static String NUMBER_REGEX_PATTERN = "(\\d)+";
    // Use a named group to return the partition in a complex string to always get the correct information for the
    // partition number.
    private final static String PARTITION_NAMED_GROUP_REGEX_PATTERN = "(?<partition>\\d)+";
    private final static String PARTITION_PATTERN = "\\{\\{partition}}";
    private final static String START_OFFSET_PATTERN = "\\{\\{start_offset}}";
    private final static String TIMESTAMP_PATTERN = "\\{\\{timestamp}}";
    private Pattern partitionPattern;
    private int taskId;
    private int maxTasks;

    /**
     *
     * @param sourceNameToBeEvaluated
     *            is the filename/table name of the source for the connector.
     * @return Predicate to confirm if the given source name matches
     */
    @Override
    public boolean isPartOfTask(final String sourceNameToBeEvaluated) {
        final Matcher match = partitionPattern.matcher(sourceNameToBeEvaluated);
        if (match.find()) {
            return toBeProcessedByThisTask(taskId, maxTasks, Integer.parseInt(match.group("partition")));
        }
        LOG.warn("Unable to find the partition from this file name {}", sourceNameToBeEvaluated);
        return false;

    }

    /**
     *
     * @param taskId
     *            what task is currently being run
     * @param expectedSourceNameFormat
     *            what the format of the source should appear like so to configure the task distribution.
     */
    @Override
    public void configureTask(final int taskId, final int maxTasks, final String expectedSourceNameFormat) {
        if (!expectedSourceNameFormat.contains(PARTITION_PATTERN)) {
            throw new ConfigException(String.format(
                    "Partition pattern %s not found, please configure the expected source to include the partition pattern.",
                    PARTITION_PATTERN));
        }
        setTaskId(taskId);
        setMaxTasks(maxTasks);
        // Build REGEX Matcher
        String regexString = StringUtils.replace(expectedSourceNameFormat, START_OFFSET_PATTERN, NUMBER_REGEX_PATTERN);
        regexString = StringUtils.replace(regexString, TIMESTAMP_PATTERN, NUMBER_REGEX_PATTERN);
        regexString = StringUtils.replace(regexString, PARTITION_PATTERN, PARTITION_NAMED_GROUP_REGEX_PATTERN);
        partitionPattern = Pattern.compile(regexString);
    }

    public void setTaskId(final int taskId) {
        this.taskId = taskId;
    }

    public void setMaxTasks(final int maxTasks) {
        this.maxTasks = maxTasks;
    }
}
