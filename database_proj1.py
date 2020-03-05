import mysql.connector
from relation_classes import *
from create_tables import *
from datetime import timedelta

mydb = mysql.connector.connect(
    host = "localhost",
    user = "root",
    password = "hoangdieu72",
    database = "Relationship2"
)

#mycursor.execute("CREATE DATABASE busSystem")

#initialize cursor of database
mycursor = mydb.cursor()
#Create Tables and Insert data
createTables(mycursor)
insertIntoRoutes(mycursor, mydb)
insertIntoTime(mycursor, mydb)
routeConstraint(mycursor, mydb)
insertIntoBusDriver(mycursor, mydb)
driverConstraint(mycursor, mydb)
insertIntoDriverAssignment( mycursor, mydb)

#get all driver_ids that have assignments
sql = ("SELECT DISTINCT DRIVER_ID FROM Driver_Assignment")
mycursor.execute(sql)
driver_ids = mycursor.fetchall()
#A driver cannot drive more than one bus at a time
sql = ("SELECT Driver_id, Driver_Assignment.Route_ID, Departure_time, Travel_time, Day_of_week " +
        "FROM Driver_Assignment, Routes " +
        "WHERE Driver_Assignment.Route_ID = Routes.Route_ID and Day_of_week = %s and Driver_ID = %s " +
        "GROUP BY Driver_id, Driver_Assignment.Route_ID, Departure_time, Travel_time, Day_of_week " +
        "ORDER BY Driver_id, Driver_Assignment.Route_ID, Departure_time, Travel_time, Day_of_week")
dayOfWeek = ['M', 'T', 'W', 'R', 'F', 'S','U']
for i in range (0, len(dayOfWeek)):
    for j in range (0, len(driver_ids)):
        executeList = []
        executeList.append(dayOfWeek[i])
        executeList.extend(driver_ids[j])
        mycursor.execute(sql, executeList)
        myResult = mycursor.fetchall()
        removeInconsistentAssignments(myResult, mycursor, mydb)
#A driver must be given enough test
for i in range (0, len(dayOfWeek)):
    for j in range (0, len(driver_ids)):
        executeList = []
        executeList.append(dayOfWeek[i])
        executeList.extend(driver_ids[j])
        mycursor.execute(sql, executeList)
        myResult = mycursor.fetchall()
        enoughRest(myResult, mycursor, mydb)

#Driver info. Ask the name or ID of a driver, and then output his/her information, as well as all the driver assignment.
mycursor.execute("SELECT DISTINCT Driver_Id FROM Bus_Driver")
driver_ids = mycursor.fetchall()
newList = []

userEnterID = input ("Please enter an ID of a bus driver")
#add driver_ids to newList with proper formatting
for driver in driver_ids:
    dS = str(driver)
    dS = dS.strip()
    dS = dS.strip('(')
    dS = dS.strip(')')
    dS = dS.strip(',')
    dS = dS.strip('\'')
    newList.append(dS)


if userEnterID in newList:
    list = []
    list.append(userEnterID)
    sql = ("SELECT DISTINCT Bus_Driver.Driver_ID, Last_name, First_name, Hometown_city, Hometown_state, Route_ID " +
           "FROM Bus_Driver, Driver_Assignment " +
           "WHERE Bus_Driver.Driver_ID = %s")
    mycursor.execute (sql, list)
    driverInformation = mycursor.fetchall()
    print ("The driver information is: ")
    for i in range (0, len(driverInformation)):
        tuple = driverInformation[i]
        if i == 0:
            print ("Driver ID:", tuple[0])
            print ("First name:", tuple[2])
            print ("Last name:", tuple[1])
            print ("Home city:", tuple[3])
            print ("Home state:", tuple[4])
            print ("Assignment(s):", tuple[5])
        else:
            print ("\t\t\t  ", tuple[5])
        i += 1
else:
    print ("Invalid user ID entered")

#City check. Ask the user the name of a city and a day of the week, list the departure and arrival for all the bus routes to/from the city, in order of time.
city = input ("Please enter the name of the city (not including spaces) to receive departure and arrival times")
day = input ("Please enter the day to receive departure and arrival times")
list = []
list.append(city)
list.append(day)
print (list[0])
dayList = ['M', 'T', 'W', 'R', 'F', 'S', 'U']
sql = ("SELECT DISTINCT Driver_Assignment.Departure_time, Routes.Travel_time " +
       "FROM Routes, Driver_Assignment " +
       "WHERE Driver_Assignment.Route_ID = Routes.Route_ID " +
       "AND Departure_city = %s AND Driver_Assignment.Day_of_week = %s " +
        " ORDER BY Departure_time")
mycursor.execute(sql, list)
depTimes = mycursor.fetchall()
for tuple in depTimes:
    print ("From", city, "The departure time is", tuple[0], "on", day)
    travelTime = tuple[1]
    totalTime = travelTime + tuple[0]
    dayListIndex = dayList.index(day)
    while totalTime >= timedelta(hours = 24):
        dayListIndex = (dayListIndex + 1) % 7
        totalTime = totalTime - timedelta(hours = 24)
    print ("The arrival time to next city is ", totalTime, "on", dayList[dayListIndex])

sql = ("SELECT DISTINCT Driver_Assignment.Departure_time, Routes.Travel_time " +
       "FROM Routes, Driver_Assignment " +
       "WHERE Driver_Assignment.Route_ID = Routes.Route_ID " +
       "AND Destination_city = %s AND Driver_Assignment.Day_of_week = %s " +
        " ORDER BY Departure_time")
mycursor.execute(sql, list)
depTimes = mycursor.fetchall()
print (depTimes)
for tuple in depTimes:
    travelTime = tuple[1]
    totalTime = travelTime + tuple[0]
    dayListIndex = dayList.index(day)
    while totalTime >= timedelta(hours = 24):
        dayListIndex = (dayListIndex + 1) % 7
        totalTime = totalTime - timedelta(hours = 24)
    print ("To", city, "The arrival time is", totalTime, "on", dayList[dayListIndex])


#Route check. Ask the user to enter the departure and destination city.
# Find all routes that will be travel from the departure to the destination city by taking at most 2 transfers.
# If a transfer is made, the second bus should department in the range of 15-75 minutes since the first one arrives.
# (For example, taking route 1 to New York and then take route 2 to Boston is consider one transfer).
# Notice that this is a query for passengers only. For drivers, the constraint above still holds.
# find all departures from city
deptCity = input ("Please enter the departure city")
destCity = input ("Please enter the destination city")
deptList = []
destList = []
destList.append(destCity)
deptList.append(deptCity)

createFirstTransferTable(mycursor, mydb, destList, deptList)
routeFinder(mycursor, mydb, destList, dayList, destCity, deptCity)
createSecondTransferTable(mycursor, mydb, destList, deptList)
secondTransfer(mycursor, mydb, destList, dayList, destCity, deptCity)



#mycursor.execute("CREATE DATABASE busSystem")

#mycursor.execute("SHOW TABLES")
#for tb in mycursor:
#    print(tb)

