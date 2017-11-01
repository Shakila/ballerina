/*
 *  Copyright (c) 2017 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */
package org.ballerinalang.nativeimpl.task;

import org.ballerinalang.nativeimpl.task.appointment.Appointment;
import org.ballerinalang.nativeimpl.task.timer.Timer;

import java.util.HashMap;
import java.util.Map;

/**
 * Maintains timers and appointments (Appointment support will be available in the future).
 */
public class TaskRegistry {

    private static TaskRegistry instance = new TaskRegistry();
    private Map<String, Timer> timers = new HashMap<>();
    private Map<String, Appointment> appointments = new HashMap<>();

    public static TaskRegistry getInstance() {
        return instance;
    }

    public void stopTask(String taskId) throws Exception {
        if (timers.containsKey(taskId)) {
            timers.get(taskId).stop();
        } else if (appointments.containsKey(taskId)) {
            appointments.get(taskId).stop();
        }
    }

    public void addTimer(Timer timer) {
        if (timers.containsKey(timer.getId())) {
            throw new IllegalArgumentException("Timer with ID " + timer.getId() + " already exists");
        }
        timers.put(timer.getId(), timer);
    }

    public Appointment getAppointment(String taskId) {
        if (!appointments.containsKey(taskId)) {
            throw new IllegalArgumentException("Appointment with ID " + taskId + " does not exist");
        }
        return appointments.get(taskId);
    }

    public void addAppointment(Appointment appointment) {
        if (appointments.containsKey(appointment.getId())) {
            throw new IllegalArgumentException("Appointment with ID " + appointment.getId() + " already exists");
        }
        appointments.put(appointment.getId(), appointment);
    }

    public void remove(String taskId) {
        if (timers.containsKey(taskId)) {
            timers.remove(taskId);
        } else if (appointments.containsKey(taskId)) {
            appointments.remove(taskId);
        }
    }

    /**
     * Returns the life time of the task if it is defined.
     *
     * @param taskId The identifier of the task.
     * @return The numeric value.
     */
    public long getExecutionLifeTime(String taskId) {
        return getAppointment(taskId).getLifeTime();
    }
}
