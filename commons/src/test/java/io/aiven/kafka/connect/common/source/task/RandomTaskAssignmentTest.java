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

import java.util.ArrayList;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

final class RandomTaskAssignmentTest {

    @ParameterizedTest
    @CsvSource({ "logs-0-0002.txt", "logs-1-0002.txt", "logs-2-0002.txt", "logs-3-0002.txt", "logs-4-0002.txt",
            "logs-5-0002.txt", "logs-6-0002.txt", "logs-7-0002.txt", "logs-8-0002.txt", "logs-9-0002.txt", "key-0.txt",
            "logs-1-0002.txt", "key-0002.txt", "logs-3-0002.txt", "key-0002.txt", "logs-5-0002.txt", "value-6-0002.txt",
            "logs-7-0002.txt", "anImage8-0002.png",
            "reallylongfilenamecreatedonS3tohisdesomedata and alsohassome spaces.txt" })
    void randomTaskAssignmentExactlyOnce(final String path) {
        final int maxTaskId = 10;
        final TaskAssignment taskAssignment = new RandomTaskAssignment();
        final List<Boolean> results = new ArrayList<>();
        for (int taskId = 1; taskId <= maxTaskId; taskId++) {
            taskAssignment.configureTask(taskId, maxTaskId, null);
            results.add(taskAssignment.isPartOfTask(path));
        }
        Assertions.assertThat(results)
                .containsExactlyInAnyOrder(Boolean.TRUE, Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, Boolean.FALSE,
                        Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, Boolean.FALSE);
    }

}
