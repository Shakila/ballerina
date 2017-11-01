import ballerina.file;
import ballerina.task;

int count;
error err;

function scheduleAppointment (int minute, int hour, int dayOfWeek, int dayOfMonth, int month) returns (string taskId, error e) {
    function () returns (error) scheduleAppointmentOnTriggerFunction;
    scheduleAppointmentOnTriggerFunction = cleanup;
    function (error) scheduleAppointmentOnErrorFunction;
    scheduleAppointmentOnErrorFunction = cleanupError;

    taskId, e = task:scheduleAppointment(scheduleAppointmentOnTriggerFunction, scheduleAppointmentOnErrorFunction, {minute:minute, hour:hour, dayOfWeek:dayOfWeek, dayOfMonth:dayOfMonth, month:month});
    return;
}

function cleanup () returns (error e) {
    file:File targetDir = {path:"/tmp/tmpDir"};
    targetDir.delete();
    boolean b = targetDir.exists();
    if (!b) {
        count = count + 1;
    } else {
        e = {msg:"Unable to clean up the tmp directory"};
    }
    return;
}

function cleanupError (error error) {
    err = error;
}

function getError() returns (error error) {
    if(err != null) {
        error = err;
    }
    return;
}
