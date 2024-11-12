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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.kafka.common.config.ConfigException;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

final class FilenamePartitionTaskAssignmentTest {

    @Test
    void partitionInFileNameDefaultAivenS3Sink() {
        final TaskAssignment taskAssignment = new FilenamePartitionTaskAssignment();
        taskAssignment.configureTask(2, 2, "logs-\\{\\{partition}}-\\{\\{start_offset}}");
        assertThat(taskAssignment.isPartOfTask("logs-1-00112.gz")).isTrue();
    }

    @Test
    void partitionLocationNotSetExpectException() {
        final TaskAssignment taskAssignment = new FilenamePartitionTaskAssignment();
        assertThrows(ConfigException.class,
                () -> taskAssignment.configureTask(1, 2, "logs-23-<partition>-<start_offset>"));

    }

    @ParameterizedTest
    @CsvSource({ "logs-\\{\\{partition}}-\\{\\{start_offset}},logs-1-00112.gz",
            "logs-2024-\\{\\{timestamp}}-\\{\\{partition}}-\\{\\{start_offset}},logs-2024-20220201-1-00112.gz",
            "logs-2023-\\{\\{partition}}-\\{\\{start_offset}},logs-2023-1-00112.gz",
            "logs1-\\{\\{timestamp}}-\\{\\{timestamp}}-\\{\\{timestamp}}-\\{\\{partition}}-\\{\\{start_offset}},logs1-2022-10-02-10-00112.gz",
            "8952\\{\\{partition}}-\\{\\{start_offset}},89521-00112.gz",
            "Emergency-TEST\\{\\{partition}}-\\{\\{start_offset}},Emergency-TEST1-00112.gz",
            "PROD-logs-\\{\\{partition}}-\\{\\{start_offset}},PROD-logs-1-00112.gz",
            "DEV_team_\\{\\{partition}}-\\{\\{start_offset}},DEV_team_1-00112.gz",
            "DEV_team_\\{\\{partition}}-\\{\\{start_offset}},DEV_team_1-00112.gz",
            "timeseries-\\{\\{partition}}-\\{\\{start_offset}},timeseries-1-00112.gz" })
    void testPartitionFileNamesAndExpectedOutcomes(final String configuredFilename, final String filename) {
        final TaskAssignment taskAssignment = new FilenamePartitionTaskAssignment();
        // This test is testing the filename matching not the task allocation.
        taskAssignment.configureTask(1, 1, configuredFilename);
        assertThat(taskAssignment.isPartOfTask(filename)).isTrue();
    }

    @ParameterizedTest
    @CsvSource({ "different-topic-\\{\\{partition}}-\\{\\{start_offset}},logs-1-00112.gz",
            "no-seperator-in-date-partition-offset-\\{\\{timestamp}}-\\{\\{partition}}-\\{\\{start_offset}},no-seperator-in-date-partition-offset-202420220201100112.gz",
            "logs-2024-\\{\\{timestamp}}-\\{\\{partition}}-\\{\\{start_offset}},logs-20201-1-00112.gz",
            "logs-2024-\\{\\{timestamp}}\\{\\{partition}}-\\{\\{start_offset}},logs-202011-00112.gz",
            "logs-2023-\\{\\{partition}}-\\{\\{start_offset}},logs-2023-one-00112.gz" })
    void expectFalseOnMalformedFilenames(final String configuredFilename, final String filename) {
        final TaskAssignment taskAssignment = new FilenamePartitionTaskAssignment();
        // This test is testing the filename matching not the task allocation.
        taskAssignment.configureTask(1, 1, configuredFilename);
        assertThat(taskAssignment.isPartOfTask(filename)).isFalse();
    }

    @ParameterizedTest
    @CsvSource({ "1,10,topics/logs/0/logs-0-0002.txt", "2,10,topics/logs/1/logs-1-0002.txt",
            "3,10,topics/logs/2/logs-2-0002.txt", "4,10,topics/logs/3/logs-3-0002.txt",
            "5,10,topics/logs/4/logs-4-0002.txt", "6,10,topics/logs/5/logs-5-0002.txt",
            "7,10,topics/logs/6/logs-6-0002.txt", "8,10,topics/logs/7/logs-7-0002.txt",
            "9,10,topics/logs/8/logs-8-0002.txt", "10,10,topics/logs/9/logs-9-0002.txt" })
    void checkCorrectDistributionAcrossTasks(final int taskId, final int maxTaskId, final String path) {

        final TaskAssignment taskAssignment = new FilenamePartitionTaskAssignment();

        taskAssignment.configureTask(taskId, maxTaskId, "logs-\\{\\{partition}}-\\{\\{start_offset}}");
        Assertions.assertThat(taskAssignment.isPartOfTask(path)).isTrue();
    }

}
