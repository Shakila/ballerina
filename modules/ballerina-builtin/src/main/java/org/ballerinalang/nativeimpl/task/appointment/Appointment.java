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
package org.ballerinalang.nativeimpl.task.appointment;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.ballerinalang.bre.Context;
import org.ballerinalang.bre.bvm.WorkerContext;
import org.ballerinalang.model.values.BString;
import org.ballerinalang.model.values.BValue;
import org.ballerinalang.nativeimpl.task.Constant;
import org.ballerinalang.nativeimpl.task.SchedulingException;
import org.ballerinalang.nativeimpl.task.TaskException;
import org.ballerinalang.nativeimpl.task.TaskIdGenerator;
import org.ballerinalang.nativeimpl.task.TaskRegistry;
import org.ballerinalang.util.codegen.ProgramFile;
import org.ballerinalang.util.codegen.cpentries.FunctionRefCPEntry;
import org.ballerinalang.util.exceptions.BLangRuntimeException;
import org.ballerinalang.util.program.BLangFunctions;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Represents an appointment.
 */
public class Appointment {
    private static final Log log = LogFactory.getLog(Appointment.class.getName());
    private String id = TaskIdGenerator.generate();
    private ScheduledExecutorService executorService = Executors.newScheduledThreadPool(Constant.POOL_SIZE);
    private Long lifeTime = 0L;
    private int minute = -1;
    private int hour = -1;
    private int dayOfWeek = -1;
    private int dayOfMonth = -1;
    private int month = -1;
    private FunctionRefCPEntry onTriggerFunction;
    private FunctionRefCPEntry onErrorFunction;
    private Context context;

    /**
     * Triggers the appointment.
     *
     * @param ctx               The ballerina context.
     * @param minute            The value of the minute in the appointment expression.
     * @param hour              The value of the hour in the appointment expression.
     * @param dayOfWeek         The value of the day of week in the appointment expression.
     * @param dayOfMonth        The value of the day of month in the appointment expression.
     * @param month             The value of the month in the appointment expression.
     * @param onTriggerFunction The main function which will be triggered by the task.
     * @param onErrorFunction   The function which will be triggered in the error situation.
     * @throws SchedulingException
     * @throws TaskException
     */
    public Appointment(Context ctx, int minute, int hour, int dayOfWeek, int dayOfMonth, int month,
                       FunctionRefCPEntry onTriggerFunction, FunctionRefCPEntry onErrorFunction)
            throws SchedulingException, TaskException {
        //Get the Calendar instance.
        Calendar currentTime = Calendar.getInstance();
        if (isInvalidInput(currentTime, minute, hour, dayOfWeek, dayOfMonth, month)) {
            throw new SchedulingException("Appointment scheduling fields should be in the correct range");
        }

        final Runnable schedulerFunc = () -> {
            try {
                if (this.lifeTime > 0) {
                    //Set the life time to 0 and trigger every minute.
                    setLifeTime(0L);
                    new Appointment(ctx, Constant.NOT_CONSIDERABLE, Constant.NOT_CONSIDERABLE, dayOfWeek,
                            dayOfMonth, month, onTriggerFunction, onErrorFunction);
                } else {
                    new Appointment(ctx, minute, hour, dayOfWeek, dayOfMonth, month, onTriggerFunction,
                            onErrorFunction);
                }
            } catch (SchedulingException | TaskException e) {
                log.error(e.getMessage());
            }
            callTriggerFunction(ctx, onTriggerFunction, onErrorFunction);
        };

        ctx.startTrackWorker();
        this.context = ctx;
        this.minute = minute;
        this.hour = hour;
        this.dayOfWeek = dayOfWeek;
        this.dayOfMonth = dayOfMonth;
        this.month = month;
        this.onTriggerFunction = onTriggerFunction;
        this.onErrorFunction = onErrorFunction;

        Calendar executionStartTime = findExecutionTime(currentTime, minute, hour, dayOfWeek, dayOfMonth, month);
        //Calculate the time difference in MILLI SECONDS.
        long delay = calculateDifference(executionStartTime);

        if (this.lifeTime > 0 && this.lifeTime < Constant.LIFETIME) {
            delay = 60000 - ((Constant.LIFETIME - this.lifeTime) % 60000);
            Calendar clonedTime = Calendar.getInstance();
            clonedTime.add(Calendar.MILLISECOND, (int) delay);
            if (clonedTime.get(Calendar.HOUR) != hour) {
                delay = 0;
            }
        }
        if (delay != -1) {
            //Schedule the task
            executorService.schedule(schedulerFunc, delay, TimeUnit.MILLISECONDS);
            ctx.startTrackWorker();
            //Get the execution life time.
            long period = this.lifeTime;
            if (period > 0) {
                //Calculate the actual execution lifetime from the delay and calculated value.
                period = delay + period;
                setLifeTime(period);
                //Trigger stop if the execution lifetime > 0.
                stopExecution(period);
            } else {
                setLifeTime(0L);
            }
            if (log.isDebugEnabled()) {
                log.debug(Constant.PREFIX_APPOINTMENT + id + " is scheduled with the DELAY [" + delay
                        + "] MILLISECONDS with the PERIOD: [" + period + "]");
            }
        }
        TaskRegistry.getInstance().addAppointment(this);
    }

    /**
     * Calls the onTrigger and onError functions.
     *
     * @param parentCtx         The ballerina context.
     * @param onTriggerFunction The main function which will be triggered by the task.
     * @param onErrorFunction   The function which will be triggered in the error situation.
     */
    private static void callTriggerFunction(Context parentCtx, FunctionRefCPEntry onTriggerFunction,
                                            FunctionRefCPEntry onErrorFunction) {
        ProgramFile programFile = parentCtx.getProgramFile();
        //Create new instance of the context and set required properties.
        Context newContext = new WorkerContext(programFile, parentCtx);
        try {
            //Invoke the onTrigger function.
            BValue[] results = BLangFunctions
                    .invokeFunction(programFile, onTriggerFunction.getFunctionInfo(), null, newContext);
            // If there are results, that mean an error has been returned
            if (onErrorFunction != null && results.length > 0 && results[0] != null) {
                BLangFunctions.invokeFunction(programFile, onErrorFunction.getFunctionInfo(), results, newContext);
            }
        } catch (BLangRuntimeException e) {
            //Call the onError function in case of error.
            if (onErrorFunction != null) {
                BValue[] error = new BValue[]{new BString(e.getMessage())};
                BLangFunctions.invokeFunction(programFile, onErrorFunction.getFunctionInfo(), error, newContext);
            }
            parentCtx.endTrackWorker();
        }
    }

    public String getId() {
        return id;
    }

    public Long getLifeTime() {
        return lifeTime;
    }

    public void setLifeTime(Long lifeTime) {
        this.lifeTime = lifeTime;
    }
    /**
     * Stops the execution.
     *
     * @throws TaskException
     * @throws SchedulingException
     */
    public void stop() throws TaskException, SchedulingException {
        TaskRegistry registry = TaskRegistry.getInstance();
        if (registry.getAppointment(id) == null) {
            throw new SchedulingException("Unable to find the corresponding task");
        }
        stopExecution(0L);
    }

    /**
     * Checks the validity of the input.
     *
     * @param currentTime The Calendar instance with current time.
     * @param minute      The value of the minute in the appointment expression.
     * @param hour        The value of the hour in the appointment expression.
     * @param dayOfWeek   The value of the day of week in the appointment expression.
     * @param dayOfMonth  The value of the day of month in the appointment expression.
     * @param month       The value of the month in the appointment expression.
     * @return boolean value. 'true' if the input is valid else 'false'.
     */
    private static boolean isInvalidInput(Calendar currentTime, int minute, int hour, int dayOfWeek, int dayOfMonth,
                                          int month) {
        //Valid ranges: (minute :- 0 - 59, hour :- 0 - 23, dayOfWeek :- 1 - 7, dayOfMonth :- 1 - 31, month :- 0 - 11).
        Calendar clonedCalendar = (Calendar) currentTime.clone();
        clonedCalendar.set(Calendar.MONTH, month);
        return minute > 59 || minute < Constant.NOT_CONSIDERABLE || hour > 23 || hour < Constant.NOT_CONSIDERABLE
                || dayOfWeek > 7 || dayOfWeek < Constant.NOT_CONSIDERABLE || dayOfWeek == 0 || dayOfMonth > 31
                || dayOfMonth < Constant.NOT_CONSIDERABLE || dayOfMonth == 0 || month > 11
                || month < Constant.NOT_CONSIDERABLE || dayOfMonth > clonedCalendar
                .getActualMaximum(Calendar.DAY_OF_MONTH);
    }

    /**
     * Returns the next execution time to schedule the appointment.
     *
     * @param currentTime The calendar instance.
     * @param minute      The value of the minute in the appointment expression.
     * @param hour        The value of the hour in the appointment expression.
     * @param dayOfWeek   The value of the day of week in the appointment expression.
     * @param dayOfMonth  The value of the day of month in the appointment expression.
     * @param month       The value of the month in the appointment expression.
     * @return updated Calendar.
     * @throws SchedulingException
     */
    public Calendar findExecutionTime(Calendar currentTime, int minute, int hour, int dayOfWeek, int dayOfMonth,
                                      int month) throws SchedulingException {
        //Clone the current time to another instance.
        Calendar executionStartTime = (Calendar) currentTime.clone();
        //Tune the execution start time by the value of minute.
        executionStartTime = modifyCalendarByCheckingMinute(currentTime, executionStartTime, minute, hour);
        //Tune the execution start time by the value of hour.
        executionStartTime = modifyCalendarByCheckingHour(currentTime, executionStartTime, minute, hour, dayOfWeek,
                dayOfMonth);
        if (dayOfWeek != Constant.NOT_CONSIDERABLE && dayOfMonth != Constant.NOT_CONSIDERABLE) {
            //Clone the modified Calendar instances into two instances.
            Calendar newTimeAccordingToDOW = cloneCalendarAndSetTime(executionStartTime, dayOfWeek, dayOfMonth);
            Calendar newTimeAccordingToDOM = cloneCalendarAndSetTime(executionStartTime, dayOfWeek, dayOfMonth);
            //Modify the specific Calendar by the value of day of week.
            newTimeAccordingToDOW = modifyCalendarByCheckingDayOfWeek(currentTime, newTimeAccordingToDOW, dayOfWeek,
                    month);
            //Modify the specific Calendar by the value of day of month.
            newTimeAccordingToDOM = modifyCalendarByCheckingDayOfMonth(currentTime, newTimeAccordingToDOM, dayOfMonth,
                    month);
            //Modify both cloned Calendar instances by the value of month.
            newTimeAccordingToDOW = modifyCalendarByCheckingMonth(currentTime, newTimeAccordingToDOW, minute, hour,
                    dayOfWeek, dayOfMonth, month);
            newTimeAccordingToDOM = modifyCalendarByCheckingMonth(currentTime, newTimeAccordingToDOM, minute, hour,
                    dayOfWeek, dayOfMonth, month);
            //Find the nearest value from both and set the final execution time.
            executionStartTime = newTimeAccordingToDOW.before(newTimeAccordingToDOM) ?
                    newTimeAccordingToDOW :
                    newTimeAccordingToDOM;
        } else {
            //Tune the execution start time by the value of day of week, day of month and month respectively.
            executionStartTime = modifyCalendarByCheckingDayOfWeek(currentTime, executionStartTime, dayOfWeek, month);
            executionStartTime = modifyCalendarByCheckingDayOfMonth(currentTime, executionStartTime, dayOfMonth, month);
            executionStartTime = modifyCalendarByCheckingMonth(currentTime, executionStartTime, minute, hour, dayOfWeek,
                    dayOfMonth, month);
        }
        return executionStartTime;
    }

    /**
     * Modifies the Calendar by checking the minute.
     *
     * @param currentTime        The Calendar instance with current time.
     * @param executionStartTime The modified Calendar instance.
     * @param minute             The value of the minute in the appointment expression.
     * @param hour               The value of the hour in the appointment expression.
     * @return updated Calendar.
     */
    private static Calendar modifyCalendarByCheckingMinute(Calendar currentTime, Calendar executionStartTime,
                                                           int minute, int hour) {
        if (minute == Constant.NOT_CONSIDERABLE && hour == Constant.NOT_CONSIDERABLE) {
            //Run every minute.
            executionStartTime.add(Calendar.MINUTE, 1);
            executionStartTime = setCalendarFields(executionStartTime, Constant.NOT_CONSIDERABLE,
                    Constant.NOT_CONSIDERABLE, Constant.NOT_CONSIDERABLE);
        } else if (minute == Constant.NOT_CONSIDERABLE) {
            //Run at clock time at 0th minute with 59 minutes execution lifetime e.g start at 2AM and end at 2.59AM.
            executionStartTime.set(Calendar.MINUTE, 0);
        } else {
            //Run every hour at 0th minute or at 5th minute or at a clock time e.g: 2.30AM.
            executionStartTime = setCalendarFields(executionStartTime, Constant.NOT_CONSIDERABLE, minute,
                    Constant.NOT_CONSIDERABLE);
            if (minute != 0 && hour == Constant.NOT_CONSIDERABLE && executionStartTime.before(currentTime)) {
                //If the modified time is behind the current time and it is every hour at 0th minute.
                //or at 5th minute case, add an hour
                executionStartTime.add(Calendar.HOUR, 1);
            }
        }
        return executionStartTime;
    }

    /**
     * Modifies the Calendar by checking the hour.
     *
     * @param currentTime        The Calendar instance with current time.
     * @param executionStartTime The modified Calendar instance.
     * @param minute             The value of the minute in the appointment expression.
     * @param hour               The value of the hour in the appointment expression.
     * @param dayOfWeek          The value of the day of week in the appointment expression.
     * @param dayOfMonth         The value of the day of month in the appointment expression.
     * @return updated Calendar.
     */
    private Calendar modifyCalendarByCheckingHour(Calendar currentTime, Calendar executionStartTime, int minute,
                                                  int hour, int dayOfWeek, int dayOfMonth) {
        if (minute == 0 && hour == Constant.NOT_CONSIDERABLE) {
            //If the minute == 0 and hour = -1, execute every hour.
            executionStartTime.add(Calendar.HOUR, 1);
            executionStartTime = setCalendarFields(executionStartTime, Constant.NOT_CONSIDERABLE, 0,
                    Constant.NOT_CONSIDERABLE);
        } else if (hour != Constant.NOT_CONSIDERABLE) {
            /*If the hour >= 12, it's in the 24 hours system.
            Therefore, find the duration to be added to the 12 hours system.*/
            executionStartTime.set(Calendar.HOUR, hour >= 12 ? hour - 12 : hour);
            executionStartTime = setCalendarFields(executionStartTime, Constant.NOT_CONSIDERABLE,
                    Constant.NOT_CONSIDERABLE, Constant.NOT_CONSIDERABLE);
            if (hour <= 11) {
                //If the hour <= 11, it's first half of the day.
                executionStartTime.set(Calendar.AM_PM, Calendar.AM);
            } else {
                //It's second half of the day
                executionStartTime.set(Calendar.AM_PM, Calendar.PM);
            }
        }
        if (executionStartTime.before(currentTime) && dayOfWeek == Constant.NOT_CONSIDERABLE
                && dayOfMonth == Constant.NOT_CONSIDERABLE) {
            if (minute == Constant.NOT_CONSIDERABLE && hour > Constant.NOT_CONSIDERABLE && (
                    currentTime.get(Calendar.HOUR) == hour || currentTime.get(Calendar.HOUR) + 12 == hour)) {
                long difference = currentTime.getTimeInMillis() - executionStartTime.getTimeInMillis();
                if (difference > 0) {
                    setLifeTime(Constant.LIFETIME - difference);
                } else {
                    setLifeTime(Constant.LIFETIME);
                }
            } else {
                //If the modified time is behind the current time, add a day.
                executionStartTime.add(Calendar.DATE, 1);
            }
        }
        return executionStartTime;
    }

    /**
     * Modifies the Calendar by checking the day of week.
     *
     * @param currentTime        The Calendar instance with current time.
     * @param executionStartTime The modified Calendar instance.
     * @param dayOfWeek          The value of the day of week in the appointment expression.
     * @param month              The value of the month in the appointment expression.
     * @return updated Calendar.
     */
    private static Calendar modifyCalendarByCheckingDayOfWeek(Calendar currentTime, Calendar executionStartTime,
                                                              int dayOfWeek, int month) {
        int numberOfDaysToBeAdded = 0;
        if (dayOfWeek >= 1) {
            if (month == currentTime.get(Calendar.MONTH) || month == Constant.NOT_CONSIDERABLE) {
                /*If the provided value of the month is current month or no value considerable value is provided
                for month, calculate the number of days to be added.*/
                if (dayOfWeek < executionStartTime.get(Calendar.DAY_OF_WEEK)) {
                    numberOfDaysToBeAdded = 7 - (executionStartTime.get(Calendar.DAY_OF_WEEK) - dayOfWeek);
                } else if (dayOfWeek > executionStartTime.get(Calendar.DAY_OF_WEEK)) {
                    numberOfDaysToBeAdded = dayOfWeek - executionStartTime.get(Calendar.DAY_OF_WEEK);
                } else if (executionStartTime.get(Calendar.DAY_OF_WEEK) == dayOfWeek && executionStartTime
                        .before(currentTime)) {
                    /*If the day of week of the execution time is same as the provided value
                    and the calculated time is behind, add 7 days.*/
                    numberOfDaysToBeAdded = 7;
                }
            } else if (currentTime.get(Calendar.MONTH) < month) {
                /*If the provided value of the month is future, find the first possible date
                which is the same day of week.*/
                executionStartTime.set(Calendar.MONTH, month);
                executionStartTime.set(Calendar.DATE, 1);
                executionStartTime = setFirstPossibleDate(executionStartTime, dayOfWeek);
            }
            executionStartTime.add(Calendar.DATE, numberOfDaysToBeAdded);
        }
        return executionStartTime;
    }

    /**
     * Modifies the Calendar by checking the day of month.
     *
     * @param currentTime        The Calendar instance with current time.
     * @param executionStartTime The modified Calendar instance.
     * @param dayOfMonth         The value of the day of month in the appointment expression.
     * @param month              The value of the month in the appointment expression.
     * @return updated Calendar.
     */
    private static Calendar modifyCalendarByCheckingDayOfMonth(Calendar currentTime, Calendar executionStartTime,
                                                               int dayOfMonth, int month) {
        if (dayOfMonth >= 1) {
            if (dayOfMonth > executionStartTime.getActualMaximum(Calendar.DAY_OF_MONTH)
                    && month == Constant.NOT_CONSIDERABLE) {
                /*If the last day of the calculated execution start time is less than the provided day of month,
                set it to next month.*/
                executionStartTime.add(Calendar.MONTH, 1);
                executionStartTime.set(Calendar.DATE, dayOfMonth);
            } else {
                //Set the day of month of execution time.
                executionStartTime.set(Calendar.DATE, dayOfMonth);
                if (executionStartTime.before(currentTime)) {
                    //If the calculated execution time is behind the current time, set it to next month.
                    executionStartTime.add(Calendar.MONTH, 1);
                }
            }
        }
        return executionStartTime;
    }

    /**
     * Modifies the Calendar by checking the month.
     *
     * @param currentTime        The Calendar instance with current time.
     * @param executionStartTime The modified Calendar instance.
     * @param minute             The value of the minute in the appointment expression.
     * @param hour               The value of the hour in the appointment expression.
     * @param dayOfWeek          The value of the day of week in the appointment expression.
     * @param dayOfMonth         The value of the day of month in the appointment expression.
     * @param month              The value of the month in the appointment expression.
     * @return updated Calendar.
     */
    private Calendar modifyCalendarByCheckingMonth(Calendar currentTime, Calendar executionStartTime, int minute,
                                                   int hour, int dayOfWeek, int dayOfMonth, int month) {
        if (minute == Constant.NOT_CONSIDERABLE && hour > Constant.NOT_CONSIDERABLE) {
            if (this.lifeTime == 0L) {
                //If the hour has considerable value and minute is -1, set the execution lifetime to 59 minutes.
                setLifeTime(Constant.LIFETIME);
            }
        }
        if (month > Constant.NOT_CONSIDERABLE) {
            if (executionStartTime.get(Calendar.MONTH) < month) {
                //Add the number of months to be added when considerable value is passed to the month.
                executionStartTime.add(Calendar.MONTH, month - executionStartTime.get(Calendar.MONTH));
                if (dayOfWeek == Constant.NOT_CONSIDERABLE && dayOfMonth == Constant.NOT_CONSIDERABLE) {
                    /*Set the date as the first day of the month if there is no considerable value for both day of week
                    and day of month.*/
                    executionStartTime.set(Calendar.DATE, 1);
                }
            } else if (executionStartTime.get(Calendar.MONTH) > month) {
                //If the month of the calculated execution start time is future, schedule it to next year.
                int months = month - executionStartTime.get(Calendar.MONTH);
                executionStartTime.add(Calendar.YEAR, 1);
                executionStartTime.add(Calendar.MONTH, months);
                if (dayOfWeek == Constant.NOT_CONSIDERABLE && dayOfMonth == Constant.NOT_CONSIDERABLE) {
                    /*Set the date as the first day of the month if there is no considerable value for both day of week
                    and day of month.*/
                    executionStartTime.set(Calendar.DATE, 1);
                }
            }
        }
        //Check the year of the calculated execution time and set it properly.
        executionStartTime = modifyCalendarByCheckingTheYear(currentTime, executionStartTime, minute, hour, dayOfWeek,
                dayOfMonth, month);
        return executionStartTime;
    }

    /**
     * Modifies the Calendar by checking the year.
     *
     * @param currentTime        The Calendar instance with current time.
     * @param executionStartTime The modified Calendar instance.
     * @param minute             The value of the minute in the appointment expression.
     * @param hour               The value of the hour in the appointment expression.
     * @param dayOfWeek          The value of the day of week in the appointment expression.
     * @param dayOfMonth         The value of the day of month in the appointment expression.
     * @param month              The value of the month in the appointment expression.
     * @return updated Calendar.
     */
    private static Calendar modifyCalendarByCheckingTheYear(Calendar currentTime, Calendar executionStartTime,
                                                            int minute, int hour, int dayOfWeek, int dayOfMonth,
                                                            int month) {
        if ((minute == Constant.NOT_CONSIDERABLE && hour == Constant.NOT_CONSIDERABLE) && (
                currentTime.get(Calendar.YEAR) < executionStartTime.get(Calendar.YEAR) || (
                        currentTime.get(Calendar.MONTH) < executionStartTime.get(Calendar.MONTH) && (
                                month != Constant.NOT_CONSIDERABLE || dayOfMonth != Constant.NOT_CONSIDERABLE)) || (
                        currentTime.get(Calendar.DAY_OF_WEEK) != executionStartTime.get(Calendar.DAY_OF_WEEK)
                                && dayOfWeek != Constant.NOT_CONSIDERABLE) || (
                        currentTime.get(Calendar.DAY_OF_MONTH) != executionStartTime.get(Calendar.DAY_OF_MONTH)
                                && dayOfMonth != Constant.NOT_CONSIDERABLE))) {
            //If the the execution start time is future, set the time to midnight (00.00.00).
            executionStartTime = setCalendarFields(executionStartTime, 0, 0, 0);
        }
        if (currentTime.get(Calendar.YEAR) < executionStartTime.get(Calendar.YEAR) && (
                (dayOfWeek != Constant.NOT_CONSIDERABLE && dayOfWeek != executionStartTime.get(Calendar.DAY_OF_WEEK))
                        || (dayOfMonth == Constant.NOT_CONSIDERABLE && currentTime.get(Calendar.DATE) != 1))) {
            /*1. If the year of the execution start time is greater than current and
            day of week is set and execution start time's day of week is not same, find the first possible day.
            2. If the year of the execution start time is greater than current and day of month is not set
            and date of the execution start time is not first day, set the date to first day.
            */
            //Set the date to first day.
            executionStartTime.set(Calendar.DATE, 1);
            if (dayOfWeek != Constant.NOT_CONSIDERABLE) {
                executionStartTime = setFirstPossibleDate(executionStartTime, dayOfWeek);
            }
        }
        return executionStartTime;
    }

    /**
     * Sets the first possible date where the day of week is same.
     *
     * @param executionStartTime The modified Calendar instance.
     * @param dayOfWeek          The value of the day of week in the appointment expression.
     * @return updated Calendar.
     */
    private static Calendar setFirstPossibleDate(Calendar executionStartTime, int dayOfWeek) {
        while (executionStartTime.get(Calendar.DAY_OF_WEEK) != dayOfWeek) {
                    /*If the execution start time is future and there is a considerable value is passed to the dayOfWeek
                    find the first possible day which is the same day of week.*/
            if (executionStartTime.get(Calendar.DAY_OF_WEEK) > dayOfWeek) {
                executionStartTime.add(Calendar.DATE, 7 - (executionStartTime.get(Calendar.DAY_OF_WEEK) - dayOfWeek));
            } else {
                executionStartTime.add(Calendar.DATE, dayOfWeek - executionStartTime.get(Calendar.DAY_OF_WEEK));
            }
        }
        return executionStartTime;
    }

    /**
     * Clone a Calendar into new instance.
     *
     * @param executionStartTime The modified Calendar instance.
     * @param dayOfWeek          The value of the day of week in the appointment expression.
     * @param dayOfMonth         The value of the day of month in the appointment expression.
     * @return updated Calendar.
     */
    private static Calendar cloneCalendarAndSetTime(Calendar executionStartTime, long dayOfWeek, long dayOfMonth) {
        //Clone the Calendar to another instance
        Calendar clonedCalendar = (Calendar) executionStartTime.clone();
        if (dayOfWeek != Constant.NOT_CONSIDERABLE && dayOfMonth != Constant.NOT_CONSIDERABLE) {
            clonedCalendar.set(Calendar.HOUR, 0);
            clonedCalendar.set(Calendar.MINUTE, 0);
            clonedCalendar.set(Calendar.SECOND, 0);
            clonedCalendar.set(Calendar.MILLISECOND, 0);
        }
        return clonedCalendar;
    }

    /**
     * Calculates the time difference in milliseconds.
     *
     * @param executionStartTime The modified Calendar instance.
     * @return duration in milliseconds.
     */
    private static long calculateDifference(Calendar executionStartTime) {
        //Calculate the time difference between current time and the calculated execution time in milli seconds.
        LocalDateTime localCurrentTime = LocalDateTime.now();
        ZoneId currentZone = ZoneId.systemDefault();
        ZonedDateTime zonedCurrentTime = ZonedDateTime.of(localCurrentTime, currentZone);
        LocalDateTime localExecutionStartTime = LocalDateTime.ofInstant(executionStartTime.toInstant(), currentZone);
        ZonedDateTime zonedExecutionStartTime = ZonedDateTime.of(localExecutionStartTime, currentZone);
        Duration duration = Duration.between(zonedCurrentTime, zonedExecutionStartTime);
        return duration.toMillis();
    }

    /**
     * @param period The delay to start the task shutdown function.
     * @throws SchedulingException
     * @throws TaskException
     */
    private void stopExecution(Long period) throws SchedulingException, TaskException {
        ScheduledExecutorService executorServiceToStopTheTask = Executors.newScheduledThreadPool(Constant.POOL_SIZE);
        final Runnable schedulerFunc = () -> {
            try {
                executorService.shutdown();
                if (executorService.isShutdown()) {
                    //Remove the executor service from the registry.
                    TaskRegistry.getInstance().remove(id);
                    if (onTriggerFunction != null && period > 0) {
                        new Appointment(context, minute, hour, dayOfWeek, dayOfMonth, month, onTriggerFunction,
                                onErrorFunction);
                    } else {
                        context.endTrackWorker();
                    }
                } else {
                    throw new SchedulingException("Unable to stop the task");
                }
            } catch (SchedulingException e) {
                log.error(e.getMessage());
            } catch (TaskException e) {
                log.error(e.getMessage());
            }
        };
        executorServiceToStopTheTask.schedule(schedulerFunc, period, TimeUnit.MILLISECONDS);
    }

    /**
     * Sets the calendar fields.
     *
     * @param calendar The Calendar instance to be modified.
     * @param ampm     The value of ampm field to set to the Calendar.
     * @param minutes  The value of the minutes to set to the Calendar.
     * @param hours    The value of the hours to set to the Calendar.
     * @return updated Calendar.
     */
    private static Calendar setCalendarFields(Calendar calendar, int ampm, int minutes, int hours) {
        if (hours != Constant.NOT_CONSIDERABLE) {
            calendar.set(Calendar.HOUR, hours);
        }
        if (minutes != Constant.NOT_CONSIDERABLE) {
            calendar.set(Calendar.MINUTE, minutes);
        }
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        if (ampm != Constant.NOT_CONSIDERABLE) {
            calendar.set(Calendar.AM_PM, ampm);
        }
        return calendar;
    }
}
