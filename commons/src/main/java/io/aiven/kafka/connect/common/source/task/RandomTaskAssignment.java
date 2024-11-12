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

public class RandomTaskAssignment implements TaskAssignment {
    private int maxTasks;
    private int taskId;
    @Override
    public boolean isPartOfTask(final String filenameToBeEvaluated) {
        final int taskAssignment = Math.floorMod(filenameToBeEvaluated.hashCode(), maxTasks);
        // floor mod returns the remainder of a division so will start at 0 and move up
        // tasks start at 1 and so we simply add one to the task assignment to get the correct thread to
        // process the task.
        return taskAssignment == taskId;
    }

    @Override
    public void configureTask(final int taskId, final int maxTasks, final String expectedFormat) {
        setMaxTasks(maxTasks);
        setTaskId(taskId);
    }

    public void setTaskId(final int taskId) {
        this.taskId = taskId;
    }

    public void setMaxTasks(final int maxTasks) {
        this.maxTasks = maxTasks;
    }
}
