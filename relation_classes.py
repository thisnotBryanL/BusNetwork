import mysql.connector

def insert_tuples(list, sqlFormula, mycursor, mydb):
    i = 0
    #print (sqlFormula)
    for r in list:
        try:
            #print (list[i])
            tup = list[i].split(',')
            #remove trailing newline
            tup[-1] = tup[-1].rstrip()
            #represent bool as int instead of string
            for j in range(0, len(tup)):
                if tup[j] == '1' or tup[j] == '0':
                    tup[j] = int(tup[j])
            mycursor.execute(sqlFormula, tup)
            i += 1
            #print ("tuple successfully inserted")
        except mysql.connector.Error as error:
            print(tup, "tuple could not be inserted")
            i += 1
    mydb.commit()

"""def SimulDriver (mycursor, mydb):
    # get all driver_ids that have assignments
    sql = ("SELECT DISTINCT DRIVER_ID FROM Driver_Assignment")
    mycursor.execute(sql)
    driver_ids = mycursor.fetchall()
    # A driver cannot drive more than one bus at a time
    sql = (SELECT DISTINCT Driver_id, Driver_Assignment.Route_ID, Departure_time, Travel_time, 
Day_of_week, ADDTIME (Departure_time, Travel_time) AS Arrival_Time 
FROM Driver_Assignment, Routes
WHERE Driver_Assignment.Route_ID = Routes.Route_ID and Driver_ID = %s
ORDER BY FIELD (Day_of_week, 'M', 'T', 'W', 'R', 'F', 'S', 'U'), Driver_Assignment.Route_ID, Travel_time, Departure_time)
    dayOfWeek = ['M', 'T', 'W', 'R', 'F', 'S', 'U']

    for i in range(0, len(dayOfWeek)):
        for j in range(0, len(driver_ids)):
            executeList = []
            executeList.append(dayOfWeek[i])
            executeList.extend(driver_ids[j])
            mycursor.execute(sql, executeList)
            myResult = mycursor.fetchall()
            cannotDriveSimul(myResult, mycursor, mydb)

    #driver cannot drive more than one bus at same time
    for r in range(0, len(myresult)):
        #convert tuple into list so it can be modified
        tupleCurr = list(myresult[r])
        #finishing time = departure time + travel time
        finishingTime = tupleCurr[2] + tupleCurr[3]
        #compare each tuple with every other tuple
        for j in range(r + 1, len(myresult)):
            executeList = []
            #print ("j is ", j)
            #print ("list len is ", len(myresult))
            tupleCmp = list(myresult[j])
            #if departure time occurs before finishingTime, delete tuple from table
            if tupleCmp[2] < finishingTime:
                #delete tuple where Driver_ID
                executeList.append(tupleCmp[0])
                #Route_ID
                executeList.append(tupleCmp[1])
                #Departure_time
                executeList.append(tupleCmp[2])
                #and Day_of_week are equal to the tuple to be deleted
                executeList.append(tupleCmp[4])
                print ("tuple removed because a bus driver"
                       " cannot drive more than one bus"
                       " at the same time", executeList)
                mycursor.execute(sqlFormula, executeList)
                mydb.commit()"""



def removeInconsistentAssignments (myresult, mycursor, mydb):
    sqlFormula = ("DELETE FROM Driver_Assignment " +
                  "WHERE DRIVER_ID = %s " +
                  "and ROUTE_ID = %s " +
                  "and Departure_time = %s " +
                  "and Day_of_week = %s")

    #driver cannot drive more than one bus at same time
    for r in range(0, len(myresult)):
        #convert tuple into list so it can be modified
        tupleCurr = list(myresult[r])
        #finishing time = departure time + travel time
        finishingTime = tupleCurr[2] + tupleCurr[3]
        #compare each tuple with every other tuple
        for j in range(r + 1, len(myresult)):
            executeList = []
            #print ("j is ", j)
            #print ("list len is ", len(myresult))
            tupleCmp = list(myresult[j])
            #if departure time occurs before finishingTime, delete tuple from table
            if tupleCmp[2] < finishingTime:
                #delete tuple where Driver_ID
                executeList.append(tupleCmp[0])
                #Route_ID
                executeList.append(tupleCmp[1])
                #Departure_time
                executeList.append(tupleCmp[2])
                #and Day_of_week are equal to the tuple to be deleted
                executeList.append(tupleCmp[4])
                print ("tuple removed because a bus driver"
                       " cannot drive more than one bus"
                       " at the same time", executeList)
                mycursor.execute(sqlFormula, executeList)
                mydb.commit()

def enoughRest(myresult, mycursor, mydb):
    sqlFormula = ("DELETE FROM Driver_Assignment " +
                  "WHERE DRIVER_ID = %s " +
                  "and ROUTE_ID = %s " +
                  "and Departure_time = %s " +
                  "and Day_of_week = %s")

    for r in range(0, len(myresult)):
        #convert tuple into list so it can be modified
        tupleCurr = list(myresult[r])
        #next Assignment Time = departure time + travel time + travel time/2
        nextAssignTime = tupleCurr[2] + tupleCurr[3] + tupleCurr[3] / 2
        #compare each tuple with every other tuple
        for j in range(r + 1, len(myresult)):
            executeList = []
            tupleCmp = list(myresult[j])
            #if departure time occurs before finishingTime, delete tuple from table
            if tupleCmp[2] < nextAssignTime:
                #delete tuple where Driver_ID
                executeList.append(tupleCmp[0])
                #Route_ID
                executeList.append(tupleCmp[1])
                #Departure_time
                executeList.append(tupleCmp[2])
                #and Day_of_week are equal to the tuple to be deleted
                executeList.append(tupleCmp[4])
                print ("tuple removed because driver"
                       " was not given enough rest", executeList)
                mycursor.execute(sqlFormula, executeList)
                mydb.commit()


