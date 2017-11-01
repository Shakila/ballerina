package ballerina.task;

@Description { value:"Schedules the task service with delay and interval"}
@Param { value:"onTrigger: This is the function which is executed while scheduling the task" }
@Param { value:"onError: This is the function which is executed in case of failure in scheduling the task" }
@Param { value:"schedule: It is a struct. Which contains the delay and interval" }
@Return { value:"string: The identifier of the scheduled task" }
@Return { value:"error: The error which is occurred while scheduling the task" }
public native function scheduleTimer (
  function() returns (error) onTrigger,
  function(error e) onError,
  struct {
    int delay = 0;
    int interval;
  } timerScheduler) returns (string taskId, error e);

@Description { value:"Schedules the task service with cron expression"}
@Param { value:"onTrigger: This is the function which is executed while scheduling the task" }
@Param { value:"onError: This is the function which is executed in case of failure in scheduling the task" }
@Param { value:"schedule: It is a struct. Which contains the minute, hour, day of week, day of month and month" }
@Return { value:"string: The identifier of the scheduled task" }
@Return { value:"error: The error which is occurred while scheduling the task" }
public native function scheduleAppointment (
function() returns (error) onTrigger,
function(error e) onError,
struct {
    int minute = -1;
    int hour = -1;
    int dayOfWeek = -1;
    int dayOfMonth = -1;
    int month = -1;
} schedule) returns (string taskId, error e);

@Description { value:"Stops the task service which corresponds to the task identifier"}
@Param { value:"taskID: The identifier of the scheduled task" }
@Return { value:"error: The error which is occurred while stopping the task" }
public native function stopTask (string taskID) returns (error);