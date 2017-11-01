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
package org.ballerinalang.test.task;

/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.ballerinalang.launcher.util.BCompileUtil;
import org.ballerinalang.launcher.util.BRunUtil;
import org.ballerinalang.launcher.util.CompileResult;
import org.ballerinalang.model.values.BInteger;
import org.ballerinalang.model.values.BString;
import org.ballerinalang.model.values.BValue;
import org.ballerinalang.nativeimpl.task.Constant;
import org.ballerinalang.nativeimpl.task.SchedulingException;
import org.ballerinalang.nativeimpl.task.TaskRegistry;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.awaitility.Awaitility.await;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Calendar;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Tests for Ballerina timer tasks.
 */
public class AppointmentTest {
    private static final Log log = LogFactory.getLog(AppointmentTest.class);
    private CompileResult appointmentCompileResult;

    @BeforeClass
    public void setup() {
        appointmentCompileResult = BCompileUtil.compileAndSetup("test-src/task/task-appointment.bal");
        printDiagnostics(appointmentCompileResult);
    }

    @Test(priority = 1, description = "Test for scheduling the Appointment to trigger every minute and stop")
    public void testTriggerAppointmentWithErrorFn() {
        CompileResult errorAppointmentCompileResult = BCompileUtil
                .compileAndSetup("test-src/task/appointment-error.bal");
        printDiagnostics(errorAppointmentCompileResult);
        BValue[] args = { new BInteger(-1), new BInteger(-1), new BInteger(-1), new BInteger(-1), new BInteger(-1) };
        BValue[] returns = BRunUtil
                .invokeStateful(errorAppointmentCompileResult, TestConstant.APPOINTMENT_ONTRIGGER_FUNCTION, args);
        String taskId = returns[0].stringValue();
        assertNotEquals(taskId, "", "Invalid task ID");  // A non-null task ID should be returned
        assertEquals(returns.length, 2); // There should be no errors
        assertNull(returns[1], "Ballerina scheduler returned an error");
        await().atMost(65, SECONDS).until(() -> {
            BValue[] error = BRunUtil.invokeStateful(errorAppointmentCompileResult, "getError");
            return error != null && error[0] != null && !error[0].stringValue().equals("");
        });
        // Now test whether the onError Ballerina function got called
        BValue[] error = BRunUtil.invokeStateful(errorAppointmentCompileResult, "getError");
        assertNotNull(error[0], "Expected error not returned.");
        assertTrue(error[0].stringValue().contains("failed to delete file"), "Expected error message not returned.");
    }

    @Test(priority = 1, description = "Test for scheduling the Appointment to trigger every minute and stop")
    public void testScheduleAppointmentEveryMinute() {
        Calendar currentTime = Calendar.getInstance();
        Calendar modifiedTime = (Calendar) currentTime.clone();
        modifiedTime.add(Calendar.MINUTE, 1);
        modifiedTime = setCalendarFields(modifiedTime, -1, 0, 0, -1, -1);
        createDirectoryWithFile();
        BValue[] args = { new BInteger(-1), new BInteger(-1), new BInteger(-1), new BInteger(-1), new BInteger(-1) };
        BValue[] returns = BRunUtil
                .invokeStateful(appointmentCompileResult, TestConstant.APPOINTMENT_ONTRIGGER_FUNCTION, args);
        String taskId = returns[0].stringValue();
        Calendar executionTime = Calendar.getInstance();
        TaskRegistry registry = TaskRegistry.getInstance();
        try {
            executionTime = registry.getAppointment(taskId).findExecutionTime(currentTime, -1, -1, -1, -1, -1);
        } catch (SchedulingException e) {
        }
        await().atMost(85, SECONDS).until(() -> {
            BValue[] count = BRunUtil.invokeStateful(appointmentCompileResult, "getCount");
            return ((BInteger) count[0]).intValue() == 0; // Only one trigger will be happened during this interval
        });
        assertNotEquals(taskId, "", "Invalid task ID");  // A non-null task ID should be returned
        assertEquals(calculateDifference(modifiedTime, executionTime) % 60000, 0,
                "The calculated execution time is not same as the expected value");
        assertNotEquals(taskId, "", "Invalid task ID");  // A non-null task ID should be returned
        assertEquals(returns.length, 2); // There should be no errors
        assertNull(returns[1], "Ballerina scheduler returned an error");
        invokeStopTaskAndCheckAssert(taskId);
    }

    @Test(description = "Test for scheduling the Appointment to trigger every minute in this hour and stop")
    public void testScheduleAppointmentToStartNowAndRunOneHour() {
        Calendar currentTime = Calendar.getInstance();
        int hour = currentTime.get(Calendar.HOUR);
        oneHourTask(hour, currentTime.get(Calendar.AM_PM));
    }

    @Test(description = "Test for scheduling the Appointment to trigger tomorrow at every minute and stop")
    public void testScheduleAppointmentEveryMinuteTomorrow() {
        String taskId;
        Calendar currentTime = Calendar.getInstance();
        Calendar clonedCalendar = (Calendar) currentTime.clone();
        clonedCalendar.add(Calendar.DATE, 1);
        int dayOfWeek = clonedCalendar.get(Calendar.DAY_OF_WEEK);
        Calendar modifiedTime = (Calendar) currentTime.clone();
        if (currentTime.get(Calendar.DAY_OF_WEEK) == dayOfWeek) {
            modifiedTime.add(Calendar.MINUTE, 1);
            modifiedTime = setCalendarFields(modifiedTime, -1, 0, 0, -1, -1);
        } else {
            modifiedTime = modifyTheCalendarAndCalculateTimeByDayOfWeek(modifiedTime, currentTime, dayOfWeek);
        }
        BValue[] args = { new BInteger(-1), new BInteger(-1), new BInteger(dayOfWeek), new BInteger(-1),
                new BInteger(-1) };
        BValue[] returns = BRunUtil
                .invokeStateful(appointmentCompileResult, TestConstant.APPOINTMENT_ONTRIGGER_FUNCTION, args);
        taskId = returns[0].stringValue();
        TaskRegistry registry = TaskRegistry.getInstance();
        Calendar executionTime = Calendar.getInstance();
        try {
            executionTime = registry.getAppointment(taskId).findExecutionTime(currentTime, -1, -1, dayOfWeek, -1, -1);
        } catch (SchedulingException e) {
        }
        checkAssert(returns, executionTime, modifiedTime);
    }

    @Test(description = "Test for scheduling the Appointment to trigger every 5th minute and stop")
    public void testScheduleAppointmentEvery5thMinute() {
        String taskId;
        Calendar currentTime = Calendar.getInstance();
        Calendar modifiedTime = (Calendar) currentTime.clone();
        modifiedTime = setCalendarFields(modifiedTime, -1, 0, 0, 5, -1);
        if (modifiedTime.before(currentTime)) {
            modifiedTime.add(Calendar.HOUR, 1);
        }
        BValue[] args = { new BInteger(5), new BInteger(-1), new BInteger(-1), new BInteger(-1), new BInteger(-1) };
        BValue[] returns = BRunUtil
                .invokeStateful(appointmentCompileResult, TestConstant.APPOINTMENT_ONTRIGGER_FUNCTION, args);
        taskId = returns[0].stringValue();
        TaskRegistry registry = TaskRegistry.getInstance();
        Calendar executionTime = Calendar.getInstance();
        try {
            executionTime = registry.getAppointment(taskId).findExecutionTime(currentTime, 5, -1, -1, -1, -1);
        } catch (SchedulingException e) {
        }
        checkAssert(returns, executionTime, modifiedTime);
    }

    @Test(description = "Test for scheduling the Appointment to trigger every hour and stop")
    public void testScheduleAppointmentEveryHour() {
        String taskId;
        Calendar currentTime = Calendar.getInstance();
        Calendar modifiedTime = (Calendar) currentTime.clone();
        modifiedTime = setCalendarFields(modifiedTime, -1, 0, 0, 0, -1);
        modifiedTime.add(Calendar.HOUR, 1);
        BValue[] args = { new BInteger(0), new BInteger(-1), new BInteger(-1), new BInteger(-1), new BInteger(-1) };
        BValue[] returns = BRunUtil
                .invokeStateful(appointmentCompileResult, TestConstant.APPOINTMENT_ONTRIGGER_FUNCTION, args);
        taskId = returns[0].stringValue();
        TaskRegistry registry = TaskRegistry.getInstance();
        Calendar executionTime = Calendar.getInstance();
        try {
            executionTime = registry.getAppointment(taskId).findExecutionTime(currentTime, 0, -1, -1, -1, -1);
        } catch (SchedulingException e) {
        }
        checkAssert(returns, executionTime, modifiedTime);
    }

    @Test(description = "Test for scheduling the Appointment to trigger 10AM everyday and stop")
    public void testScheduleAppointmentEveryday10AM() {
        String taskId;
        int hour = 10;
        Calendar currentTime = Calendar.getInstance();
        Calendar modifiedTime = (Calendar) currentTime.clone();
        modifiedTime = setCalendarFields(modifiedTime, 0, 0, 0, 0, hour);
        if (modifiedTime.before(currentTime)) {
            modifiedTime.add(Calendar.DATE, 1);
        }
        BValue[] args = { new BInteger(0), new BInteger(hour), new BInteger(-1), new BInteger(-1), new BInteger(-1) };
        BValue[] returns = BRunUtil
                .invokeStateful(appointmentCompileResult, TestConstant.APPOINTMENT_ONTRIGGER_FUNCTION, args);
        taskId = returns[0].stringValue();
        TaskRegistry registry = TaskRegistry.getInstance();
        Calendar executionTime = Calendar.getInstance();
        try {
            executionTime = registry.getAppointment(taskId).findExecutionTime(currentTime, 0, hour, -1, -1, -1);
        } catch (SchedulingException e) {
        }
        checkAssert(returns, executionTime, modifiedTime);
    }

    @Test(description = "Test for 'scheduleAppointment' function to trigger at 10AM and stop at 10.59AM")
    public void testScheduleAppointmentStartAt10AMEndAt1059AM() {
        int hour = 10;
        oneHourTask(hour, 0);
    }

    @Test(description = "Test for scheduling the Appointment to trigger every minute on Mondays and stop")
    public void testScheduleAppointmentEveryMinuteOnMondays() {
        String taskId;
        int dayOfWeek = 2;
        Calendar currentTime = Calendar.getInstance();
        Calendar modifiedTime = (Calendar) currentTime.clone();
        if (currentTime.get(Calendar.DAY_OF_WEEK) == dayOfWeek) {
            modifiedTime.add(Calendar.MINUTE, 1);
            modifiedTime = setCalendarFields(modifiedTime, -1, 0, 0, -1, -1);
        } else {
            modifiedTime = modifyTheCalendarAndCalculateTimeByDayOfWeek(modifiedTime, currentTime, dayOfWeek);
        }
        BValue[] args = { new BInteger(-1), new BInteger(-1), new BInteger(dayOfWeek), new BInteger(-1),
                new BInteger(-1) };
        BValue[] returns = BRunUtil
                .invokeStateful(appointmentCompileResult, TestConstant.APPOINTMENT_ONTRIGGER_FUNCTION, args);
        taskId = returns[0].stringValue();
        TaskRegistry registry = TaskRegistry.getInstance();
        Calendar executionTime = Calendar.getInstance();
        try {
            executionTime = registry.getAppointment(taskId).findExecutionTime(currentTime, -1, -1, dayOfWeek, -1, -1);
        } catch (SchedulingException e) {
        }
        checkAssert(returns, executionTime, modifiedTime);
    }

    @Test(description = "Test for scheduling the Appointment to trigger 2PM on Mondays and stop")
    public void testScheduleAppointment2PMOnMondays() {
        String taskId;
        int dayOfWeek = 2;
        Calendar currentTime = Calendar.getInstance();
        Calendar modifiedTime = (Calendar) currentTime.clone();
        if (currentTime.get(Calendar.DAY_OF_WEEK) != dayOfWeek) {
            int days = currentTime.get(Calendar.DAY_OF_WEEK) > dayOfWeek ?
                    7 - (currentTime.get(Calendar.DAY_OF_WEEK) - dayOfWeek) :
                    dayOfWeek - currentTime.get(Calendar.DAY_OF_WEEK);
            modifiedTime.add(Calendar.DATE, days);
        }
        modifiedTime = setCalendarFields(modifiedTime, 1, 0, 0, 0, 2);
        if (modifiedTime.before(currentTime)) {
            modifiedTime.add(Calendar.DATE, 7);
        }
        BValue[] args = { new BInteger(0), new BInteger(14), new BInteger(dayOfWeek), new BInteger(-1),
                new BInteger(-1) };
        BValue[] returns = BRunUtil
                .invokeStateful(appointmentCompileResult, TestConstant.APPOINTMENT_ONTRIGGER_FUNCTION, args);
        taskId = returns[0].stringValue();
        TaskRegistry registry = TaskRegistry.getInstance();
        Calendar executionTime = Calendar.getInstance();
        try {
            executionTime = registry.getAppointment(taskId).findExecutionTime(currentTime, 0, 14, dayOfWeek, -1, -1);
        } catch (SchedulingException e) {
        }
        checkAssert(returns, executionTime, modifiedTime);
    }

    @Test(description = "Test for scheduling the Appointment to trigger next week and stop. The hour is one hour back "
            + "from the current hour")
    public void testScheduleAppointmentBeforeAnHourInNextWeek() {
        String taskId;
        Calendar currentTime = Calendar.getInstance();
        Calendar clonedCalendar = (Calendar) currentTime.clone();
        clonedCalendar.add(Calendar.HOUR, -1);
        int hour = clonedCalendar.get(Calendar.AM_PM) == 0 ?
                clonedCalendar.get(Calendar.HOUR) :
                clonedCalendar.get(Calendar.HOUR) + clonedCalendar.get(Calendar.HOUR);
        int dayOfWeek = currentTime.get(Calendar.DAY_OF_WEEK);
        Calendar modifiedTime = (Calendar) currentTime.clone();
        modifiedTime = setCalendarFields(modifiedTime, 0, 0, 0, 0, hour);
        if (modifiedTime.before(currentTime)) {
            modifiedTime.add(Calendar.DATE, 7);
        }
        BValue[] args = { new BInteger(0), new BInteger(hour), new BInteger(dayOfWeek), new BInteger(-1),
                new BInteger(-1) };
        BValue[] returns = BRunUtil
                .invokeStateful(appointmentCompileResult, TestConstant.APPOINTMENT_ONTRIGGER_FUNCTION, args);
        taskId = returns[0].stringValue();
        TaskRegistry registry = TaskRegistry.getInstance();
        Calendar executionTime = Calendar.getInstance();
        try {
            executionTime = registry.getAppointment(taskId).findExecutionTime(currentTime, 0, hour, dayOfWeek, -1, -1);
        } catch (SchedulingException e) {
        }
        checkAssert(returns, executionTime, modifiedTime);
    }

    @Test(description = "Test for scheduling the Appointment to trigger next week and stop. The day of week is "
            + "one day back from the current day of week")
    public void testScheduleAppointmentBeforeADOWInNextWeek() {
        String taskId;
        Calendar currentTime = Calendar.getInstance();
        Calendar clonedCalendar = (Calendar) currentTime.clone();
        clonedCalendar.add(Calendar.DATE, 6);
        int dayOfWeek = clonedCalendar.get(Calendar.DAY_OF_WEEK);
        Calendar modifiedTime = (Calendar) currentTime.clone();
        modifiedTime.setTime(clonedCalendar.getTime());
        modifiedTime = setCalendarFields(modifiedTime, 0, 0, 0, 0, 0);
        BValue[] args = { new BInteger(-1), new BInteger(-1), new BInteger(dayOfWeek), new BInteger(-1),
                new BInteger(-1) };
        BValue[] returns = BRunUtil
                .invokeStateful(appointmentCompileResult, TestConstant.APPOINTMENT_ONTRIGGER_FUNCTION, args);
        taskId = returns[0].stringValue();
        TaskRegistry registry = TaskRegistry.getInstance();
        Calendar executionTime = Calendar.getInstance();
        try {
            executionTime = registry.getAppointment(taskId).findExecutionTime(currentTime, -1, -1, dayOfWeek, -1, -1);
        } catch (SchedulingException e) {
        }
        checkAssert(returns, executionTime, modifiedTime);
    }

    @Test(description = "Test for scheduling the Appointment to trigger on 31st day and stop")
    public void testScheduleAppointmentOn31st() {
        String taskId;
        int dayOfMonth = 31;
        Calendar currentTime = Calendar.getInstance();
        Calendar modifiedTime = (Calendar) currentTime.clone();
        if (currentTime.get(Calendar.DAY_OF_MONTH) != dayOfMonth) {
            modifiedTime = setCalendarFields(modifiedTime, 0, 0, 0, 0, 0);
            modifiedTime = modifyTheCalendarByDayOfMonth(modifiedTime, currentTime, dayOfMonth);
        } else {
            modifiedTime.add(Calendar.MINUTE, 1);
            modifiedTime = setCalendarFields(modifiedTime, -1, 0, 0, -1, -1);
        }
        BValue[] args = { new BInteger(-1), new BInteger(-1), new BInteger(-1), new BInteger(dayOfMonth),
                new BInteger(-1) };
        BValue[] returns = BRunUtil
                .invokeStateful(appointmentCompileResult, TestConstant.APPOINTMENT_ONTRIGGER_FUNCTION, args);
        taskId = returns[0].stringValue();
        TaskRegistry registry = TaskRegistry.getInstance();
        Calendar executionTime = Calendar.getInstance();
        try {
            executionTime = registry.getAppointment(taskId).findExecutionTime(currentTime, -1, -1, -1, dayOfMonth, -1);
        } catch (SchedulingException e) {
        }
        checkAssert(returns, executionTime, modifiedTime);
    }

    @Test(description = "Test for scheduling the Appointment to trigger every minute in January and stop")
    public void testScheduleAppointmentEveryMinuteInJanuary() {
        String taskId;
        int month = 0;
        Calendar currentTime = Calendar.getInstance();
        Calendar modifiedTime = (Calendar) currentTime.clone();
        if (currentTime.get(Calendar.MONTH) != month) {
            modifiedTime = modifyTheCalendarAndCalculateTimeByMonth(modifiedTime, currentTime, month);
        } else {
            modifiedTime.add(Calendar.MINUTE, 1);
            modifiedTime = setCalendarFields(modifiedTime, -1, 0, 0, -1, -1);
        }
        BValue[] args = { new BInteger(-1), new BInteger(-1), new BInteger(-1), new BInteger(-1), new BInteger(month) };
        BValue[] returns = BRunUtil
                .invokeStateful(appointmentCompileResult, TestConstant.APPOINTMENT_ONTRIGGER_FUNCTION, args);
        taskId = returns[0].stringValue();
        TaskRegistry registry = TaskRegistry.getInstance();
        Calendar executionTime = Calendar.getInstance();
        try {
            executionTime = registry.getAppointment(taskId).findExecutionTime(currentTime, -1, -1, -1, -1, month);
        } catch (SchedulingException e) {
        }
        checkAssert(returns, executionTime, modifiedTime);
    }

    @Test(description = "Test for scheduling the Appointment to trigger every minute in next month and stop")
    public void testScheduleAppointmentEveryMinuteInNextMonth() {
        String taskId;
        int month = (Calendar.getInstance()).get(Calendar.MONTH) + 1;
        Calendar currentTime = Calendar.getInstance();
        Calendar modifiedTime = (Calendar) currentTime.clone();
        if (currentTime.get(Calendar.MONTH) != month) {
            modifiedTime = modifyTheCalendarAndCalculateTimeByMonth(modifiedTime, currentTime, month);
        } else {
            modifiedTime.add(Calendar.MINUTE, 1);
            modifiedTime = setCalendarFields(modifiedTime, -1, 0, 0, -1, -1);
        }
        BValue[] args = { new BInteger(-1), new BInteger(-1), new BInteger(-1), new BInteger(-1), new BInteger(month) };
        BValue[] returns = BRunUtil
                .invokeStateful(appointmentCompileResult, TestConstant.APPOINTMENT_ONTRIGGER_FUNCTION, args);
        taskId = returns[0].stringValue();
        TaskRegistry registry = TaskRegistry.getInstance();
        Calendar executionTime = Calendar.getInstance();
        try {
            executionTime = registry.getAppointment(taskId).findExecutionTime(currentTime, -1, -1, -1, -1, month);
        } catch (SchedulingException e) {
        }
        checkAssert(returns, executionTime, modifiedTime);
    }

    @Test(description = "Test for scheduling the Appointment to trigger next hour from current in next month and stop")
    public void testScheduleAppointmentNextHourInNextMonth() {
        String taskId;
        Calendar currentTime = Calendar.getInstance();
        Calendar clonedCalendar = (Calendar) currentTime.clone();
        clonedCalendar.add(Calendar.HOUR, 1);
        clonedCalendar.set(Calendar.DATE, 1);
        clonedCalendar.add(Calendar.MONTH, 1);
        int hour = clonedCalendar.get(Calendar.AM_PM) == 0 ?
                clonedCalendar.get(Calendar.HOUR) :
                clonedCalendar.get(Calendar.HOUR) + 12;
        int month = clonedCalendar.get(Calendar.MONTH);
        Calendar modifiedTime = (Calendar) currentTime.clone();
        modifiedTime.setTime(clonedCalendar.getTime());
        modifiedTime = setCalendarFields(modifiedTime, clonedCalendar.get(Calendar.AM_PM), 0, 0, 0, -1);
        BValue[] args = { new BInteger(-1), new BInteger(hour), new BInteger(-1), new BInteger(-1),
                new BInteger(month) };
        BValue[] returns = BRunUtil
                .invokeStateful(appointmentCompileResult, TestConstant.APPOINTMENT_ONTRIGGER_FUNCTION, args);
        taskId = returns[0].stringValue();
        TaskRegistry registry = TaskRegistry.getInstance();
        Calendar executionTime = Calendar.getInstance();
        try {
            executionTime = registry.getAppointment(taskId).findExecutionTime(currentTime, -1, hour, -1, -1, month);
        } catch (SchedulingException e) {
        }
        checkAssert(returns, executionTime, modifiedTime);
    }

    @Test(description = "Test for scheduling the Appointment to trigger next month and stop. " +
            "The day of week is one day later from current day of week")
    public void testScheduleAppointmentOnNextDOWInNextMonth() {
        testAppointmentWithDifferentDOW('+', 1);
    }

    @Test(description = "Test for scheduling the Appointment to trigger next month and stop. " +
            "The day of week is one day before from current day of week")
    public void testScheduleAppointmentOnDifferentBeforeOneDOWInNextMonth() {
        testAppointmentWithDifferentDOW('-', -1);
    }

    @Test(description = "Test for scheduling the Appointment to trigger 2PM on Tuesdays in January and stop")
    public void testScheduleAppointment2PMOnTuesdaysInJanuary() {
        String taskId;
        BValue[] args = { new BInteger(0), new BInteger(14), new BInteger(3), new BInteger(-1), new BInteger(0) };
        BValue[] returns = BRunUtil
                .invokeStateful(appointmentCompileResult, TestConstant.APPOINTMENT_ONTRIGGER_FUNCTION, args);
        taskId = returns[0].stringValue();
        assertNotEquals(taskId, "", "Invalid task ID");  // A non-null task ID should be returned
        assertEquals(returns.length, 2); // There should be no errors
        assertNull(returns[1], "Ballerina scheduler returned an error");
        invokeStopTaskAndCheckAssert(taskId);
    }

    @Test(description = "Test for scheduling the Appointment to trigger 2PM in January and stop. "
            + "The day of week is one day before from current")
    public void testScheduleAppointment2PMOnOneDayOfWeekBeforeInJanuary() {
        String taskId;
        Calendar currentTime = Calendar.getInstance();
        if (currentTime.get(Calendar.MONTH) != 0) {
            currentTime.set(Calendar.DATE, 1);
            currentTime.set(Calendar.MONTH, 0);
            currentTime.add(Calendar.YEAR, 1);
        }
        int dayOfWeek = currentTime.get(Calendar.DAY_OF_WEEK) == 1 ? 7 : currentTime.get(Calendar.DAY_OF_WEEK) - 1;
        BValue[] args = { new BInteger(0), new BInteger(14), new BInteger(dayOfWeek), new BInteger(-1),
                new BInteger(0) };
        BValue[] returns = BRunUtil
                .invokeStateful(appointmentCompileResult, TestConstant.APPOINTMENT_ONTRIGGER_FUNCTION, args);
        taskId = returns[0].stringValue();
        assertNotEquals(taskId, "", "Invalid task ID");  // A non-null task ID should be returned
        assertEquals(returns.length, 2); // There should be no errors
        assertNull(returns[1], "Ballerina scheduler returned an error");
        invokeStopTaskAndCheckAssert(taskId);
    }

    @Test(description = "Test for scheduling the Appointment to trigger 25th of every month and stop")
    public void testScheduleAppointment25thOfEachMonth() {
        String taskId;
        int dayOfMonth = 25;
        Calendar currentTime = Calendar.getInstance();
        Calendar modifiedTime = (Calendar) currentTime.clone();
        if (currentTime.get(Calendar.DAY_OF_MONTH) != dayOfMonth) {
            modifiedTime = setCalendarFields(modifiedTime, 0, 0, 0, 0, 0);
            modifiedTime = modifyTheCalendarByDayOfMonth(modifiedTime, currentTime, dayOfMonth);
        } else {
            modifiedTime.add(Calendar.MINUTE, 1);
            modifiedTime = setCalendarFields(modifiedTime, -1, 0, 0, -1, -1);
        }
        BValue[] args = { new BInteger(-1), new BInteger(-1), new BInteger(-1), new BInteger(dayOfMonth),
                new BInteger(-1) };
        BValue[] returns = BRunUtil
                .invokeStateful(appointmentCompileResult, TestConstant.APPOINTMENT_ONTRIGGER_FUNCTION, args);
        taskId = returns[0].stringValue();
        TaskRegistry registry = TaskRegistry.getInstance();
        Calendar executionTime = Calendar.getInstance();
        try {
            executionTime = registry.getAppointment(taskId).findExecutionTime(currentTime, -1, -1, -1, dayOfMonth, -1);
        } catch (SchedulingException e) {
        }
        checkAssert(returns, executionTime, modifiedTime);
    }

    @Test(description = "Test for scheduling the Appointment to trigger at midnight on Mondays "
            + "and 15th of every month and stop")
    public void testScheduleAppointmentMidnightOnMondayAnd15thOfEachMonth() {
        String taskId;
        int dayOfWeek = 2;
        int dayOfMonth = 15;
        Calendar currentTime = Calendar.getInstance();
        Calendar modifiedTimeByDOW = (Calendar) currentTime.clone();
        Calendar modifiedTimeByDOM = (Calendar) currentTime.clone();
        if (currentTime.get(Calendar.DAY_OF_WEEK) != dayOfWeek) {
            int days = currentTime.get(Calendar.DAY_OF_WEEK) > dayOfWeek ?
                    7 - (currentTime.get(Calendar.DAY_OF_WEEK) - dayOfWeek) :
                    dayOfWeek - currentTime.get(Calendar.DAY_OF_WEEK);
            modifiedTimeByDOW.add(Calendar.DATE, days);
        }
        modifiedTimeByDOW = setCalendarFields(modifiedTimeByDOW, 0, 0, 0, 0, 0);
        if (modifiedTimeByDOW.before(currentTime)) {
            modifiedTimeByDOW.add(Calendar.DATE, 7);
        }
        modifiedTimeByDOM = setCalendarFields(modifiedTimeByDOM, 0, 0, 0, 0, 0);
        modifiedTimeByDOM = modifyTheCalendarByDayOfMonth(modifiedTimeByDOM, currentTime, dayOfMonth);
        Calendar expectedTime = modifiedTimeByDOW.getTimeInMillis() < modifiedTimeByDOM.getTimeInMillis() ?
                modifiedTimeByDOW :
                modifiedTimeByDOM;
        BValue[] args = { new BInteger(0), new BInteger(0), new BInteger(dayOfWeek), new BInteger(dayOfMonth),
                new BInteger(-1) };
        BValue[] returns = BRunUtil
                .invokeStateful(appointmentCompileResult, TestConstant.APPOINTMENT_ONTRIGGER_FUNCTION, args);
        taskId = returns[0].stringValue();
        TaskRegistry registry = TaskRegistry.getInstance();
        Calendar executionTime = Calendar.getInstance();
        try {
            executionTime = registry.getAppointment(taskId)
                    .findExecutionTime(currentTime, 0, 0, dayOfWeek, dayOfMonth, -1);
        } catch (SchedulingException e) {
        }
        checkAssert(returns, executionTime, expectedTime);
    }

    @Test(description = "Test for scheduling the Appointment to trigger next year and stop. "
            + "The hour is one hour before and month is one month before from current")
    public void testScheduleAppointmentBeforeAnHourAndOneMonthInNextYear() {
        String taskId;
        Calendar currentTime = Calendar.getInstance();
        Calendar clonedCalendar = (Calendar) currentTime.clone();
        clonedCalendar.add(Calendar.HOUR, -1);
        clonedCalendar.add(Calendar.MONTH, -1);
        clonedCalendar.set(Calendar.YEAR, currentTime.get(Calendar.YEAR) + 1);
        int hour = clonedCalendar.get(Calendar.AM_PM) == 0 ?
                clonedCalendar.get(Calendar.HOUR) :
                clonedCalendar.get(Calendar.HOUR) + 12;
        int dayOfMonth = clonedCalendar.get(Calendar.DAY_OF_MONTH);
        int month = clonedCalendar.get(Calendar.MONTH);
        Calendar modifiedTime = (Calendar) currentTime.clone();
        modifiedTime.setTime(clonedCalendar.getTime());
        modifiedTime = setCalendarFields(modifiedTime, clonedCalendar.get(Calendar.AM_PM), 0, 0, 0, -1);
        BValue[] args = { new BInteger(-1), new BInteger(hour), new BInteger(-1), new BInteger(dayOfMonth),
                new BInteger(month) };
        BValue[] returns = BRunUtil
                .invokeStateful(appointmentCompileResult, TestConstant.APPOINTMENT_ONTRIGGER_FUNCTION, args);
        taskId = returns[0].stringValue();
        TaskRegistry registry = TaskRegistry.getInstance();
        Calendar executionTime = Calendar.getInstance();
        try {
            executionTime = registry.getAppointment(taskId)
                    .findExecutionTime(currentTime, -1, hour, -1, dayOfMonth, month);
        } catch (SchedulingException e) {
        }
        checkAssert(returns, executionTime, modifiedTime);
    }

    private void oneHourTask(int hour, int ampm) {
        printDiagnostics(appointmentCompileResult);
        Calendar currentTime = Calendar.getInstance();
        Calendar modifiedTime = (Calendar) currentTime.clone();
        modifiedTime = setCalendarFields(modifiedTime, ampm, 0, 0, 0, hour);
        String taskId;
        long expectedPeriod;
        long delay;
        int twentyfourSystemHour = ampm == 1 ? hour + 12 : hour;
        if (currentTime.get(Calendar.HOUR) == hour) {
            long difference = currentTime.getTimeInMillis() - modifiedTime.getTimeInMillis();
            Calendar clonedTime = (Calendar) currentTime.clone();
            clonedTime.add(Calendar.MINUTE, 1);
            clonedTime = setCalendarFields(clonedTime, ampm, 0, 0, -1, -1);
            long calculatedDelay = clonedTime.getTimeInMillis() - currentTime.getTimeInMillis();
            expectedPeriod = calculatedDelay + Constant.LIFETIME - difference;
        } else {
            if (modifiedTime.before(currentTime)) {
                modifiedTime.add(Calendar.DATE, 1);
            }
            expectedPeriod = Constant.LIFETIME + (modifiedTime.getTimeInMillis() - currentTime.getTimeInMillis());
        }
        BValue[] args = { new BInteger(-1), new BInteger(twentyfourSystemHour), new BInteger(-1), new BInteger(-1),
                new BInteger(-1) };
        BValue[] returns = BRunUtil
                .invokeStateful(appointmentCompileResult, TestConstant.APPOINTMENT_ONTRIGGER_FUNCTION, args);
        taskId = returns[0].stringValue();
        TaskRegistry registry = TaskRegistry.getInstance();
        Calendar executionTime = Calendar.getInstance();
        try {
            executionTime = registry.getAppointment(taskId)
                    .findExecutionTime(currentTime, -1, twentyfourSystemHour, -1, -1, -1);
        } catch (SchedulingException e) {
        }
        long period;
        if (currentTime.get(Calendar.HOUR) == hour && currentTime.get(Calendar.AM_PM) == ampm) {
            Calendar clonedTime = (Calendar) currentTime.clone();
            clonedTime.add(Calendar.MINUTE, 1);
            clonedTime = setCalendarFields(clonedTime, ampm, 0, 0, -1, -1);
            delay = clonedTime.getTimeInMillis() - currentTime.getTimeInMillis();
            period = delay + registry.getExecutionLifeTime(taskId);
        } else {
            period = registry.getExecutionLifeTime(taskId);
        }
        checkAssert(returns, executionTime, modifiedTime);
        assertTrue(isAcceptable(period, expectedPeriod),
                "The calculated execution life time is not the expected value");
    }

    private void testAppointmentWithDifferentDOW(char c, int days) {
        String taskId;
        Calendar currentTime = Calendar.getInstance();
        Calendar clonedCalendar = (Calendar) currentTime.clone();
        clonedCalendar.add(Calendar.MONTH, 1);
        clonedCalendar.set(Calendar.DATE, 1);
        if (clonedCalendar.get(Calendar.DAY_OF_WEEK) == currentTime.get(Calendar.DAY_OF_WEEK)) {
            clonedCalendar.add(Calendar.DATE, days);
        } else if (c == '+' && clonedCalendar.get(Calendar.DAY_OF_WEEK) < currentTime.get(Calendar.DAY_OF_WEEK)) {
            days = currentTime.get(Calendar.DAY_OF_WEEK) - clonedCalendar.get(Calendar.DAY_OF_WEEK) + 1;
            clonedCalendar.add(Calendar.DATE, days);
        } else if (c == '-' && clonedCalendar.get(Calendar.DAY_OF_WEEK) > currentTime.get(Calendar.DAY_OF_WEEK)) {
            days = 7 - (clonedCalendar.get(Calendar.DAY_OF_WEEK) - currentTime.get(Calendar.DAY_OF_WEEK)) - 1;
            clonedCalendar.add(Calendar.DATE, days);
        }
        int dayOfWeek = clonedCalendar.get(Calendar.DAY_OF_WEEK);
        int month = clonedCalendar.get(Calendar.MONTH);
        Calendar modifiedTime = (Calendar) currentTime.clone();
        modifiedTime.setTime(clonedCalendar.getTime());
        modifiedTime = setCalendarFields(modifiedTime, 0, 0, 0, 0, 0);
        BValue[] args = { new BInteger(-1), new BInteger(-1), new BInteger(dayOfWeek), new BInteger(-1),
                new BInteger(month) };
        BValue[] returns = BRunUtil
                .invokeStateful(appointmentCompileResult, TestConstant.APPOINTMENT_ONTRIGGER_FUNCTION, args);
        taskId = returns[0].stringValue();
        TaskRegistry registry = TaskRegistry.getInstance();
        Calendar executionTime = Calendar.getInstance();
        try {
            executionTime = registry.getAppointment(taskId)
                    .findExecutionTime(currentTime, -1, -1, dayOfWeek, -1, month);
        } catch (SchedulingException e) {
        }
        checkAssert(returns, executionTime, modifiedTime);
    }

    private void printDiagnostics(CompileResult timerCompileResult) {
        Arrays.asList(timerCompileResult.getDiagnostics()).
                forEach(e -> log.info(e.getMessage() + " : " + e.getPosition()));
    }

    private Calendar setCalendarFields(Calendar calendar, int ampm, int milliseconds, int seconds, int minutes,
                                       int hours) {
        if (hours != -1) {
            calendar.set(Calendar.HOUR, hours);
        }
        if (minutes != -1) {
            calendar.set(Calendar.MINUTE, minutes);
        }
        calendar.set(Calendar.SECOND, seconds);
        calendar.set(Calendar.MILLISECOND, milliseconds);
        if (ampm != -1) {
            calendar.set(Calendar.AM_PM, ampm);
        }
        return calendar;
    }

    private void createDirectoryWithFile() {
        try {
            File dir = new File(TestConstant.DIRECTORY_PATH);
            dir.mkdir();
            File.createTempFile("test", "txt", new File(dir.getAbsolutePath()));
        } catch (IOException e) {
            log.error("Unable to create the test file: " + e.getMessage());
        }
    }

    private boolean isAcceptable(long expectedDuration, long calculatedDuration) {
        return Math.abs(expectedDuration - calculatedDuration) <= 1000;
    }

    private long calculateDifference(Calendar calendar1, Calendar calendar2) {
        ZoneId currentZone = ZoneId.systemDefault();
        LocalDateTime localTime1 = LocalDateTime.ofInstant(calendar1.toInstant(), currentZone);
        ZonedDateTime zonedTime1 = ZonedDateTime.of(localTime1, currentZone);
        LocalDateTime localTime2 = LocalDateTime.ofInstant(calendar2.toInstant(), currentZone);
        ZonedDateTime zonedTime2 = ZonedDateTime.of(localTime2, currentZone);
        Duration duration = Duration.between(zonedTime1, zonedTime2);
        return duration.toMillis();
    }

    private Calendar modifyTheCalendarByDayOfMonth(Calendar calendar, Calendar currentTime, int dayOfMonth) {
        if (dayOfMonth > calendar.getActualMaximum(Calendar.DAY_OF_MONTH)) {
            calendar.add(Calendar.MONTH, 1);
            calendar.set(Calendar.DATE, dayOfMonth);
        } else {
            calendar.set(Calendar.DATE, dayOfMonth);
            if (calendar.before(currentTime)) {
                calendar.add(Calendar.MONTH, 1);
            }
        }
        return calendar;
    }

    private Calendar modifyTheCalendarAndCalculateTimeByDayOfWeek(Calendar calendar, Calendar currentTime,
                                                                  int dayOfWeek) {
        if (currentTime.get(Calendar.DAY_OF_WEEK) != dayOfWeek) {
            int days = currentTime.get(Calendar.DAY_OF_WEEK) > dayOfWeek ?
                    7 - (currentTime.get(Calendar.DAY_OF_WEEK) - dayOfWeek) :
                    dayOfWeek - currentTime.get(Calendar.DAY_OF_WEEK);
            calendar.add(Calendar.DATE, days);
            calendar = setCalendarFields(calendar, 0, 0, 0, 0, 0);
            if (calendar.before(currentTime)) {
                calendar.add(Calendar.DATE, 7);
            }
        }
        return calendar;
    }

    private Calendar modifyTheCalendarAndCalculateTimeByMonth(Calendar calendar, Calendar currentTime, int month) {
        if (calendar.get(Calendar.MONTH) > month) {
            calendar.add(Calendar.YEAR, 1);
        }
        calendar.set(Calendar.MONTH, month);
        calendar.set(Calendar.DATE, 1);
        calendar = setCalendarFields(calendar, 0, 0, 0, 0, 0);
        return calendar;
    }

    private void checkAssert(BValue[] returns, Calendar executionTime, Calendar modifiedTime) {
        String taskId = returns[0].stringValue();
        assertEquals(returns.length, 2); // There should be no errors
        assertNull(returns[1], "Ballerina scheduler returned an error");
        assertNotEquals(taskId, "", "Invalid task ID");  // A non-null task ID should be returned
        assertEquals(executionTime.getTimeInMillis(), modifiedTime.getTimeInMillis(),
                "The calculated execution time is not same as the the expected value");
        invokeStopTaskAndCheckAssert(taskId);
    }

    private void invokeStopTaskAndCheckAssert(String taskId) {
        // Now let's try stopping the task
        BValue[] stopResult = BRunUtil
                .invokeStateful(appointmentCompileResult, "stopTask", new BValue[] { new BString(taskId) });
        assertNull(stopResult[0], "Task stopping resulted in an error");
        // One more check to see whether the task really stopped
        BValue[] counts = BRunUtil.invokeStateful(appointmentCompileResult, "getCount");
        assertEquals(((BInteger) counts[0]).intValue(), -1, "Count hasn't been reset");
    }
}
