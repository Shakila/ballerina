import ballerina.data.sql;

@Description {value:"This is the Employee struct. The field names of this should match column names of the table. The field types should match with sql types."}
struct Employee {
    int id;
    string name;
    float salary;
    boolean status;
    string birthdate;
    string birthtime;
    string updated;
}

function main (string[] args) {
    sql:ClientConnector empDB;
    //Create a SQL connector by providing the required database connection
    //pool properties.
    sql:ConnectionProperties properties = {maximumPoolSize:5};
    empDB = create sql:ClientConnector(
            sql:MYSQL, "localhost", 3306, "testdb", "root", "root", properties);
    sql:Parameter[] params = [];

    //Create table named EMPLOYEE and populate sample data.
    int count = empDB.update("CREATE TABLE IF NOT EXISTS EMPLOYEE (id INT,name
        VARCHAR(25),salary DOUBLE,status BOOLEAN,birthdate DATE,birthtime TIME,
        updated TIMESTAMP)", params);
    count = empDB.update("INSERT INTO EMPLOYEE VALUES(1, 'John', 1050.50, false,
        '1990-12-31', '11:30:45', '2007-05-23 09:15:28')", params);
    count = empDB.update("INSERT INTO EMPLOYEE VALUES(2, 'Anne', 4060.50, true,
        '1999-12-31', '13:40:24', '2017-05-23 09:15:28')", params);

    //Query the table using SQL connector select action. Either select or call
    //action can return a datatable.
    datatable dt = empDB.select("SELECT * from EMPLOYEE", params);
    //Iterate through the result until hasNext() become false and retrieve
    //the data struct corresponding to each row.
    while (dt.hasNext()) {
        any dataStruct = dt.getNext();
        var rs, _ = (Employee)dataStruct;
        println("Employee:" + rs.id + "|" + rs.name +
                "|" + rs.salary + "|" + rs.status +
                "|" + rs.birthdate +
                "|" + rs.birthtime +
                "|" + rs.updated);
    }

    //Convert a datatable to JSON.
    dt = empDB.select("SELECT id,name FROM EMPLOYEE", params);
    var jsonRes, _ = <json>dt;
    println(jsonRes);

    //Convert a datatable to XML.
    dt = empDB.select("SELECT id,name FROM EMPLOYEE", params);
    var xmlRes, _ = <xml>dt;
    println(xmlRes);

    //Finally close the DB connection.
    empDB.close();
}
