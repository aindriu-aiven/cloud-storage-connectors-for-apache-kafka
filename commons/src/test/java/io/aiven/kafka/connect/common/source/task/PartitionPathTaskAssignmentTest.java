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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.kafka.connect.errors.ConnectException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

final class PartitionPathTaskAssignmentTest {

    @ParameterizedTest
    @CsvSource({ "1,1,topics/logs/partition=5/logs+5+0002.txt,true",
            "1,4,topics/logs/partition=5/logs+5+0002.txt,false", "2,4,topics/logs/partition=5/logs+5+0002.txt,true",
            "1,3,topics/logs/partition=5/logs+5+0002.txt,false", "1,5,topics/logs/partition=5/logs+5+0002.txt,true",
            "3,3,topics/logs/partition=5/logs+5+0002.txt,true" })
    void withLeadingStringPartitionNamingConvention(final int taskId, final int maxTaskId, final String path,
            final boolean expectedResult) {

        final PartitionPathTaskAssignment taskAssignment = new PartitionPathTaskAssignment();

        taskAssignment.configureTask(taskId, maxTaskId, "topics/logs/partition=\\{\\{partition}}/");
        assertThat(taskAssignment.isPartOfTask(path)).isEqualTo(expectedResult);
    }

    @ParameterizedTest
    @CsvSource({ "1,1,bucket/topics/topic-1/5/logs+5+0002.txt,true",
            "1,4,bucket/topics/topic-1/5/logs+5+0002.txt,false", "2,4,bucket/topics/topic-1/5/logs+5+0002.txt,true",
            "1,3,bucket/topics/topic-1/5/logs+5+0002.txt,false", "1,5,bucket/topics/topic-1/5/logs+5+0002.txt,true",
            "3,3,bucket/topics/topic-1/5/logs+5+0002.txt,true" })
    void partitionInPathConvention(final int taskId, final int maxTaskId, final String path,
            final boolean expectedResult) {

        final PartitionPathTaskAssignment taskAssignment = new PartitionPathTaskAssignment();

        taskAssignment.configureTask(taskId, maxTaskId, "bucket/topics/topic-1/\\{\\{partition}}/");
        assertThat(taskAssignment.isPartOfTask(path)).isEqualTo(expectedResult);
    }

    @ParameterizedTest
    @CsvSource({ "1,10,topics/logs/0/logs-0002.txt", "2,10,topics/logs/1/logs-0002.txt",
            "3,10,topics/logs/2/logs-0002.txt", "4,10,topics/logs/3/logs-0002.txt", "5,10,topics/logs/4/logs-0002.txt",
            "6,10,topics/logs/5/logs-0002.txt", "7,10,topics/logs/6/logs-0002.txt", "8,10,topics/logs/7/logs-0002.txt",
            "9,10,topics/logs/8/logs-0002.txt", "10,10,topics/logs/9/logs-0002.txt" })
    void checkCorrectDistributionAcrossTasks(final int taskId, final int maxTaskId, final String path) {

        final PartitionPathTaskAssignment taskAssignment = new PartitionPathTaskAssignment();

        taskAssignment.configureTask(taskId, maxTaskId, "topics/logs/\\{\\{partition}}/");
        assertThat(taskAssignment.isPartOfTask(path)).isTrue();
    }

    @ParameterizedTest
    @CsvSource({ "1,10,topcs/logs/0/logs-0002.txt", "2,10,topics/logs/1", "3,10,S3/logs/2/logs-0002.txt",
            "4,10,topics/log/3/logs-0002.txt", "5,10,prod/logs/4/logs-0002.txt", "6,10,misspelt/logs/5/logs-0002.txt",
            "7,10,test/logs/6/logs-0002.txt", "8,10,random/logs/7/logs-0002.txt", "9,10,DEV/logs/8/logs-0002.txt",
            "10,10,poll/logs/9/logs-0002.txt" })
    void expectNoMatchOnUnconfiguredPaths(final int taskId, final int maxTaskId, final String path) {

        final PartitionPathTaskAssignment taskAssignment = new PartitionPathTaskAssignment();

        taskAssignment.configureTask(taskId, maxTaskId, "topics/logs/\\{\\{partition}}/");
        assertThat(taskAssignment.isPartOfTask(path)).isFalse();
    }

    @Test
    void expectExceptionOnNonIntPartitionSupplied() {
        final int taskId = 1;
        final int maxTaskId = 1;
        final String path = "topics/logs/one/test-001.txt";

        final PartitionPathTaskAssignment taskAssignment = new PartitionPathTaskAssignment();

        taskAssignment.configureTask(taskId, maxTaskId, "topics/logs/\\{\\{partition}}/");
        assertThrows(ConnectException.class, () -> taskAssignment.isPartOfTask(path));
    }

}
