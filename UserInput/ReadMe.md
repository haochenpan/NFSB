## Four actions

#### load

explanation:  perfom insert into database.
```bash
#First parameter:
1.default:
This means that this workload file already exists in all the GNF instance.
2. New:
This means that you create a new workfile under the main (NFSB) directory.

#Second parameter:
The workload filename created (just the filename, do not include any path)

#Third parameter:
all:
all the GNF will run the current command.
ip1 ip2 ip3...
Only the GNF address that user put in will run the workload.
```

#### run

explanation: perform either insert to or read from the database

Parameters are the same as load

#### interrupt

explanation: stop the GNF instance

```bash
#First parameter:
all:
Interrupt all the GNF
ip1 ip2 ip3...
Interrupt only the GNF address that user put in will run the workload.
```

#### quit
explanation: shut down all the GNF

#### All the details will also be output onto terminal once the user starts the UserInput Program
