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

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PartitionPathTaskDistribution allows source connectors to distribute tasks based off the folder structure that
 * partition number that is defined in that structure. for example /PREFIX/partition={{partition}}/YYYY/MM/DD/mm this
 * will split all tasks by the number of unique partitions defined in the storage path. e.g. Task distribution in
 * Connect with 10 Partitions and 3 tasks |Task | Partition| |1|0| |2|1| |3|2| |1|3| |2|4| |3|5| |1|6| |2|7| |3|8| |1|9|
 */
public class PartitionPathTaskAssignment implements TaskAssignment {
    public static final String PARTITION_NUMBER_PATTERN = "\\{\\{partition}}";
    private final static Logger LOG = LoggerFactory.getLogger(PartitionPathTaskAssignment.class);

    private String prefix;
    private int maxTasks;
    private int taskId;

    @Override
    public boolean isPartOfTask(final String valueToBeEvaluated) {
        if (!valueToBeEvaluated.startsWith(prefix)) {
            LOG.warn("Ignoring path {}, does not contain the preconfigured prefix {} set up at startup",
                    valueToBeEvaluated, prefix);
            return false;
        }
        String value = StringUtils.substringAfter(valueToBeEvaluated, prefix);
        if (!value.contains("/")) {
            LOG.warn("Ignoring path {}, does not contain any sub folders after partitionId prefix {}",
                    valueToBeEvaluated, prefix);
            return false;
        }
        value = StringUtils.substringBefore(value, "/");

        try {
            return toBeProcessedByThisTask(taskId, maxTasks, Integer.parseInt(value));
        } catch (NumberFormatException ex) {
            throw new ConnectException(String
                    .format("Unexpected non integer value found parsing path for partitionId: %s", valueToBeEvaluated));
        }
    }

    /**
     *
     * @param taskId
     *            The task identifier
     * @param expectedPathFormat
     *            The format of the path and where to identify
     */

    @Override
    public void configureTask(final int taskId, final int maxTasks, final String expectedPathFormat) {
        setMaxTasks(maxTasks);
        setTaskId(taskId);
        if (!expectedPathFormat.contains(PARTITION_NUMBER_PATTERN)) {
            throw new ConfigException(String.format(
                    "Expected path format is missing the identifier '%s' to correctly select the partition",
                    PARTITION_NUMBER_PATTERN));
        }
        prefix = StringUtils.substringBefore(expectedPathFormat, PARTITION_NUMBER_PATTERN);

    }

    public void setMaxTasks(final int maxTasks) {
        this.maxTasks = maxTasks;
    }

    public void setTaskId(final int taskId) {
        this.taskId = taskId;
    }
}
