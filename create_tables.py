import mysql.connector
from relation_classes import *
from datetime import timedelta, datetime

def createTables(mycursor):
    mycursor.execute("CREATE TABLE IF NOT EXISTS Routes(Route_ID VARCHAR (255),"
                     "Departure_city VARCHAR (255),"
                     "Destination_city VARCHAR (255),"
                     "Departure_state VARCHAR (255),"
                     "Destination_state VARCHAR (255),"
                     "Travel_time TIME,"
                     "Weekday_only BIT,"
                     "Fare SMALLINT,"
                     "UNIQUE KEY routes_uk (Departure_city, Destination_city, Travel_time),"
                     "PRIMARY KEY routes_pk (Route_ID))")


    mycursor.execute("CREATE TABLE IF NOT EXISTS TimeTable(Route_ID VARCHAR (20),"
                     "Departure_time TIME,"
                     "Run_in_weekdays VARCHAR (255),"
                     "Run_in_weekends VARCHAR (255),"
                     "FOREIGN KEY (Route_ID) REFERENCES Routes (Route_ID),"
                     "UNIQUE KEY (Route_ID, Departure_time, Run_in_weekdays),"
                     "UNIQUE KEY (Route_ID, Departure_time, Run_in_weekdays),"
                     "PRIMARY KEY (Route_ID, Departure_time, Run_in_weekdays))")


    mycursor.execute("CREATE TABLE IF NOT EXISTS Bus_Driver (Driver_ID CHAR (6),"
                     "Last_name VARCHAR (30),"
                     "First_name VARCHAR (30),"
                     "Hometown_city VARCHAR (30),"
                     "Hometown_state VARCHAR (20),"
                     "PRIMARY KEY (Driver_ID))")

    mycursor.execute("CREATE TABLE IF NOT EXISTS Driver_Assignment (Driver_ID CHAR (6),"
                     "Route_ID VARCHAR (30),"
                     "Departure_time TIME,"
                     "Day_of_week CHAR (1),"
                     "FOREIGN KEY (Route_ID, Departure_Time) REFERENCES TimeTable(Route_ID, Departure_Time),"
                     "FOREIGN KEY(Driver_ID) REFERENCES Bus_Driver (Driver_ID),"
                     "PRIMARY KEY (Driver_ID, Route_ID, Departure_time, Day_of_week ))")

def insertIntoRoutes(mycursor, mydb):
    #insert data into Routes table
    try:
        routeFileName = input ("Please enter the file name for the Routes table ")
        routeFile = open(routeFileName, 'r')
        routeList = routeFile.readlines()
        sqlFormula = "INSERT INTO Routes (Route_ID, Departure_city, Destination_city, Departure_state, Destination_state, Travel_time, Weekday_only, Fare) VALUES (%s, %s, %s, %s, %s, %s,%s, %s)"
        insert_tuples(routeList, sqlFormula, mycursor, mydb)
    except FileNotFoundError:
        print ("File does not exist")

def insertIntoTime(mycursor, mydb):
    #insert data into Time table
    sql = """SELECT Weekday_only
    FROM Routes
    WHERE Route_ID = %s"""

    try:
        TimeFileName = input("Please enter the file name for the Time table ")
        TimeFile = open(TimeFileName, 'r')
        with open(TimeFileName) as file:
            for tuple in TimeFile:
                try:
                    routeList = []
                    executeList = []
                    #convert string into tuple
                    tuple = tuple.strip()
                    tupl = tuple.split(',')
                    #append attributes to list as appropriate data type
                    executeList.append(str(tupl[0]))
                    executeList.append(tupl[1])
                    executeList.append(int(tupl[2]))
                    executeList.append(int(tupl[3]))
                    routeid = tupl[0]
                    rweekdays = tupl[2]
                    rweekends = tupl[3]
                    routeList.append(routeid)
                    #find weekday_only attribute for corresponding Route_ID in Routes table
                    mycursor.execute(sql, routeList)
                    #if routeList not empty
                    RouteWeekDayOnly = mycursor.fetchone()
                    if RouteWeekDayOnly != None:
                        #if Week_day_only == 1, tuple cannot run on weekends and must run on weekdays
                        if RouteWeekDayOnly[0] == 1:
                            #if route runs on weekends, reject tuple
                            if executeList[3] == 1:
                                print ("Error, route cannot run on weekends. Rejecting Tuple", executeList)
                            #if route doesn't run on weekdays, reject tuple
                            elif executeList[2] == 0:
                                print("Error, route must run on weekdays. Rejecting Tuple", executeList)
                        #if both run_in_weeknds and run_in_weekdays are false, reject tuple
                        elif executeList[2] == 0 and executeList[3] == 0:
                            print("Error, route is in time table but does not run on weekends on data. ",
                                  "Rejecting tuple", executeList)

                        else:
                            sqlFormula = "INSERT INTO TimeTable (Route_ID, Departure_time, Run_in_weekdays, Run_in_weekends) VALUES (%s, %s, %s, %s)"
                            mycursor.execute(sqlFormula, executeList)
                            #print ("tuple inserted", executeList)
                            mydb.commit()
                    else:
                        print ("Referential integrity error. Rejecting tuple", executeList)
                except mysql.connector.Error as error:
                    print(executeList, "tuple could not be inserted")
                except IndexError:
                    print ("Attempting to enter invalid Data into Times Table. Rejecting tuple.")
    except FileNotFoundError:
            print("File does not exist")

def routeConstraint(mycursor, mydb):
    sql = """SELECT DISTINCT Route_ID
FROM Routes r
WHERE Route_ID NOT IN(
SELECT t.Route_ID
FROM TimeTable t
WHERE r.Route_ID = t.Route_ID)"""
    mycursor.execute(sql)
    RoutesThatDoNotBelong = mycursor.fetchall()
    if RoutesThatDoNotBelong != None:
        print("These routes do not have corresponding timetable entries:")
        for route in RoutesThatDoNotBelong:
            print (route[0])

def driverConstraint(mycursor, mydb):
    sql = """SELECT DISTINCT Route_ID
FROM Driver_Assignment d
WHERE Route_ID NOT IN(
SELECT t.Route_ID
FROM TimeTable t
WHERE d.Route_ID = t.Route_ID)"""
    mycursor.execute(sql)
    RoutesThatDoNotBelong = mycursor.fetchall()
    if RoutesThatDoNotBelong != None:
        print("Drivers were assigned to the following routes but there are no corresponding",
              "entries in the Time Table:")
        for route in RoutesThatDoNotBelong:
            print (route[0])
    sql = """SELECT Driver_ID
FROM Bus_Driver b
WHERE Driver_ID NOT IN(
SELECT d.Driver_ID
FROM Driver_Assignment d
WHERE d.Driver_ID = b.Driver_ID)"""
    mycursor.execute(sql)
    DriversWNoAssign = mycursor.fetchall()
    if DriversWNoAssign != None:
        print("These drivers have no assignments:")
        for driver in DriversWNoAssign:
            print (driver[0])


def insertIntoBusDriver(mycursor, mydb):
    #enter bus driver table data
    try:
        busDriverFileName = input ("Please enter the file name for the Bus Driver table ")
        busDriverFile = open(busDriverFileName, 'r')
        busDiverList = busDriverFile.readlines()
        sqlFormula = "INSERT INTO Bus_Driver (Driver_ID, Last_name, First_name, Hometown_city, Hometown_state) VALUES (%s, %s, %s, %s, %s)"
        insert_tuples(busDiverList, sqlFormula, mycursor, mydb)


    except FileNotFoundError:
        print("File does not exist")

def insertIntoDriverAssignment(mycursor, mydb):
    #insert data into Routes table
    try:
        driverAssignFileName = input ("Please enter the file name for the Driver Assignment table ")
        driverAssignFile = open(driverAssignFileName, 'r')
        assignList = driverAssignFile.readlines()
        sqlFormula = "INSERT INTO Driver_Assignment (Driver_ID, Route_ID, Departure_time, Day_of_week) VALUES (%s, %s, %s, %s)"
        insert_tuples(assignList, sqlFormula, mycursor, mydb)

    except FileNotFoundError:
        print ("File does not exist")

def createFirstTransferTable(mycursor, mydb, destList, deptList):
    sql = ("""SELECT DISTINCT Destination_city, ADDTIME (Departure_time, Travel_time) as Arrival_time, Routes.Route_ID, Day_of_week
FROM Driver_Assignment, Routes
WHERE Departure_city = %s AND Driver_Assignment.Route_ID = Routes.Route_ID""")
    mycursor.execute(sql, deptList)
    firstTransferResults = mycursor.fetchall()
    dayList = ['M', 'T', 'W', 'R', 'F', 'S', 'U']
    # create table and insert first transfer into table

    mycursor.execute("DROP TABLE IF EXISTS First_Transfer")
    mycursor.execute("CREATE TABLE IF NOT EXISTS First_Transfer(Arr_city VARCHAR (100),"
                     "Arrival_Time TIME,"
                     "Routeid VARCHAR (100),"
                     "dayOfWeek CHAR (1),"
                     "PRIMARY KEY (Arr_city, Arrival_Time, RouteiD, dayofWeek))")
    sqlFormula = "INSERT INTO First_Transfer (Arr_city, Arrival_Time, Routeid, dayofWeek) VALUES (%s, %s, %s, %s)"
    #mycursor.execute("INSERT INTO First_Transfer VALUES ('Philadelphia','23:55','1','W'), ('Philadelphia','23:55','1','M'), ('Philadelphia','23:55','1','T')")
    #mycursor.execute("INSERT INTO Driver_Assignment VALUES ('100000','1','23:55','W'), ('100000','1','23:55','M'), ('100000','1','23:55','T')")
    #mycursor.execute("INSERT INTO Driver_Assignment VALUES ('100000', '5', '21:00', 'W'), ('100000', '5', '22:00', 'W'), ('100000', '5', '23:00', 'W')")
    #mycursor.execute("INSERT INTO Driver_Assignment VALUES ('100000', '2A', '23:00', 'M'), ('100000', '2A', '22:00', 'M'), ('100000', '2A', '23:00', 'T')")
    #mycursor.execute("INSERT INTO Driver_Assignment VALUES ('100004', '3', '23:00', 'M'), ('100004', '3', '23:00', 'T')")
    #mycursor.execute("INSERT INTO Driver_Assignment VALUES ('100004', '3', '6:15', 'M'), ('100004', '3', '5:30', 'T')")
    #mycursor.execute("INSERT INTO Driver_Assignment VALUES ('100006', '5', '10:00', 'W')")


    for r in firstTransferResults:
        try:
            executeList = []
            arrivalTime = timedelta(hours=0)
            arrivalDay = ''
            # print ("r:", r)
            for attribute in r:
                executeList.append(attribute)
            # find index of day in dayList
            dayListIndex = dayList.index(r[3])
            arrivalTime = r[1]
            # if arrival time is more than 24 hours, go to next day
            while arrivalTime >= timedelta(hours=24):
                dayListIndex = (dayListIndex + 1) % 7
                # update arrival time
                arrivalTime = arrivalTime - timedelta(hours=24)
                # update arrival day
                arrivalDay = dayList[dayListIndex]

                # correct arrival time and arrival day
                executeList[1] = arrivalTime
                # print ("arrival time", arrivalTime)
                executeList[3] = arrivalDay
            mycursor.execute(sqlFormula, executeList)
            mydb.commit()
        except mysql.connector.Error as error:
            print()

def routeFinder(mycursor, mydb, destList, dayList, destCity, deptCity):
    #find second transfer that meets constraints
    sql = """SELECT Arrival_Time, Routeid, dayOfWeek, Day_of_week, Departure_Time, Routes.Route_ID, Arr_city
    FROM First_Transfer, Routes, Driver_Assignment
    WHERE First_Transfer.Arr_city = Routes.Departure_city AND Routes.Destination_city = %s
    AND Routes.Route_ID = Driver_Assignment.Route_ID"""
    mycursor.execute(sql, destList)
    routeFromDeptToDest = mycursor.fetchall()
    #check to make sure proper time constraints are net
    for tuple in routeFromDeptToDest:
        dayListIndexArr = dayList.index(tuple[2])
        dayListIndexDept= dayList.index(tuple[3])
        #if day of week from city_transfer table = day of week from driver_assignment
        #table, if the dept_time - arr_time < 01:15:00 AND dept_time - arr_time > 00:15:00
        #data is okay
        if tuple[2] == tuple[3]:
            arr_time = tuple[0]
            dept_time = tuple[4]
            if dept_time - arr_time < timedelta(hours = 1, minutes = 25) and dept_time - arr_time > timedelta (minutes = 15):
                print ("Take route", tuple[1], "which will arrive in", tuple[6], "on", tuple[2],
                       "at", tuple[0], ".Then take route", tuple[5], "which will depart at", tuple[4],
                       "on", tuple[3], "to", destCity)
        #check to see if departure day occurs on the next day
        elif dayListIndexDept - dayListIndexArr == 1:
            #if arrival time between 22:45:00 and 24:00:00 then a next day departure may be acceptable
            if tuple[0] >= timedelta(hours = 22, minutes = 45) and tuple[0] <= timedelta(hours = 24):
                #if so, then the arrival and departure times must be between (15, 75) minutes apart
                timeDiff = tuple[4] - tuple[0] + timedelta(hours = 24)
                if timeDiff >= timedelta(minutes = 15) and timeDiff <= timedelta(hours = 1, minutes = 15):
                    print("Take route", tuple[1], "which will arrive in", tuple[6], "on", tuple[2],
                          "at", tuple[0], ".Then take route", tuple[5], "which will depart at", tuple[4],
                          "on", tuple[3], "to", destCity)
    #find direct route
    sql = """SELECT DISTINCT Routes.Route_ID, Departure_time, Day_of_week
    FROM Driver_Assignment, Routes
    WHERE Departure_city = %s AND Destination_city = %s
    AND Driver_Assignment.Route_ID = Routes.Route_ID"""
    list = []
    list.append(deptCity)
    list.append(destCity)
    mycursor.execute(sql, list)
    directRouteList = mycursor.fetchall()
    for tuple in directRouteList:
        print("Take route", tuple[0], "which will arrive in", destCity, "on", tuple[2])

def createSecondTransferTable(mycursor, mydb, destList, deptList):
    sql = ("""SELECT DISTINCT Destination_city, ADDTIME (Departure_time, Travel_time) as Arrival_time, Routes.Route_ID, Day_of_week, First_Transfer.Routeid, Departure_time
FROM Driver_Assignment, Routes, First_Transfer
WHERE Driver_Assignment.Route_ID = Routes.Route_ID AND First_Transfer.Arr_city = Routes.Departure_city 
AND SUBTIME(Departure_time, Arrival_time) < '01:15:00' AND SUBTIME(Departure_time, Arrival_time) > '00:15:00'""")
    sqlFormula = "INSERT INTO Second_Transfer (Arr_city, Arrival_Time, Routeid, dayofWeek, FirstRouteID) VALUES (%s, %s, %s, %s, %s)"

    mycursor.execute(sql)
    secondTransferResults = mycursor.fetchall()
    dayList = ['M', 'T', 'W', 'R', 'F', 'S', 'U']
    # create table and insert first transfer into table

    mycursor.execute("DROP TABLE IF EXISTS Second_Transfer")
    mycursor.execute("CREATE TABLE IF NOT EXISTS Second_Transfer(Arr_city VARCHAR (100),"
                     "Arrival_Time TIME,"
                     "Routeid VARCHAR (100),"
                     "dayOfWeek CHAR (1),"
                     "FirstRouteID VARCHAR (100),"
                     "PRIMARY KEY (Arr_city, Arrival_Time, RouteiD, dayofWeek))")
    for r in secondTransferResults:
        try:
            executeList = []
            arrivalTime = timedelta(hours=0)
            arrivalDay = ''
            # print ("r:", r)
            for attribute in r:
                executeList.append(attribute)
            # find index of day in dayList
            dayListIndex = dayList.index(r[3])
            arrivalTime = r[1]
            # if arrival time is more than 24 hours, go to next day
            while arrivalTime >= timedelta(hours=24):
                dayListIndex = (dayListIndex + 1) % 7
                # update arrival time
                arrivalTime = arrivalTime - timedelta(hours=24)
                # update arrival day
                arrivalDay = dayList[dayListIndex]

                # correct arrival time and arrival day
                executeList[1] = arrivalTime
                # print ("arrival time", arrivalTime)
                executeList[3] = arrivalDay
            mycursor.execute(sqlFormula, executeList)
            mydb.commit()
        except mysql.connector.Error as error:
            print()

def secondTransfer(mycursor, mydb, destList, dayList, destCity, deptCity):
    # find second transfer that meets constraints
    sql = """SELECT Arrival_Time, Routeid, dayOfWeek, Day_of_week, Departure_Time, Routes.Route_ID, FirstRouteID
    FROM Second_Transfer, Routes, Driver_Assignment
    WHERE Second_Transfer.Arr_city = Routes.Departure_city AND Routes.Destination_city = %s
    AND Routes.Route_ID = Driver_Assignment.Route_ID"""
    mycursor.execute(sql, destList)
    secondTransferList = mycursor.fetchall()
    # check to make sure proper time constraints are net
    for tuple in secondTransferList:
        print (tuple)
        dayListIndexArr = dayList.index(tuple[2])
        dayListIndexDept = dayList.index(tuple[3])
        # if day of week from city_transfer table = day of week from driver_assignment
        # table, if the dept_time - arr_time < 01:15:00 AND dept_time - arr_time > 00:15:00
        # data is okay
        if tuple[2] == tuple[3]:
            arr_time = tuple[0]
            dept_time = tuple[4]
            if dept_time - arr_time < timedelta(hours=1, minutes=25) and dept_time - arr_time > timedelta(minutes=15):
                print("Take route", tuple[7], "to", tuple[1], "which will arrive in", tuple[6], "on", tuple[2],
                      "at", tuple[0], ".Then take route", tuple[5], "which will depart at", tuple[4],
                      "on", tuple[3], "to", destCity)
        # check to see if departure day occurs on the next day
        elif dayListIndexDept - dayListIndexArr == 1:
            # if arrival time between 22:45:00 and 24:00:00 then a next day departure may be acceptable
            if tuple[0] >= timedelta(hours=22, minutes=45) and tuple[0] <= timedelta(hours=24):
                # if so, then the arrival and departure times must be between (15, 75) minutes apart
                timeDiff = tuple[4] - tuple[0] + timedelta(hours=24)
                if timeDiff >= timedelta(minutes=15) and timeDiff <= timedelta(hours=1, minutes=15):
                    print("Take route", tuple[7], "to", "which will arrive in", tuple[6], "on", tuple[2],
                          "at", tuple[0], ".Then take route", tuple[5], "which will depart at", tuple[4],
                          "on", tuple[3], "to", destCity)