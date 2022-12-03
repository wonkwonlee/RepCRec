# Replicated Concurrency Control and Recovery (RepCRec)
Final project for Advanced Database Systems, Fall 2022

Young Il Kim (yk2494@nyu.edu), Wonkwon Lee (wl2733@nyu.edu)

## How to Run
1. Make sure to have Python version 3.6+ installed.
2. At the root of the project, run `python3 main.py [input file]`.\
   For example, `python3 main.py test/test1`.
3. The program will execute line by line from the `input file`.\
   If the `input file` is not provided, the program will print out error message.

  
## Data Structures and Test Specifications
- Data: The data consists of 20 distinct variables x1, ..., x20. There are 10 sites numbered 1 to 10. 
A copy is indicated by a dot. The odd indexed variables are at one site each (i.e. 1 + (index number mod 10) ). 
Even indexed variables are at all sites. Each variable xi is initialized to the value 10i (10 times i). 
Each site has an independent lock table. If that site fails, the lock table is erased.
- Input: input instructions come from a standard input python main.py [input file]. The program will read the file line by line. 
If the line starts with “//” or “===”, it will be ignored. 
If the input file is not given or doesn’t exist, then the program will show an error message. 
Each line inside the file will have at most a single instruction from one transaction or a fail, recover, dump, end, etc.
- Output: Output of each input file will be shown on the terminal. Execute each test case starting with the initial state of the database. 
The output contains (i) the committed state of the data items at each dump, (ii) which value each read returns, (iii) which transactions commit and which abort. 
Some of these operations may be blocked due to conﬂicting locks. When a transaction is waiting, it will not receive another operation.

## Algorithms
- Available copies algorithm to replication by using strict two phase locking at each site and validation at commit time.
- Each variable lock is acquired in a first-come first-serve.
- The blocking (waits-for) graph will have edges according to the transaction's execution order.
- Deadlock detection 
   - DFS algorithms to find cycle in the blocking graph to detect deadlock and abort the youngest transaction in the cycle by keeping track of the transaction time of 
   any transaction holding a lock.
   - Write of a replicated variable does not acquire a lock on any sites S unless it can acquire a lock on all the sites containing that variable to prevent needless deadlocks.
   - Deadlock detection happens at the beginning of the tick.
- Read only transactions use multiversion read consistency. For every version of xi, on each site S, record when that version was committed. 
Situations where a read only transaction read RO an item xi are as follows :
   - If xi is not replicated and the site holding xi is up, then the read only transaction can read it.
   - If xi is replicated then RO can read xi from site s if xi was committed at s by some transaction T’ before RO began and s was up all the time 
   between the time when xi was commited and RO began. In that case RO can read the version that T’ wrote. If there is no such site then RO can abort.
- At the same time, the TM will record the failure history of every site.

## Major Components
1. Main
   - Read input file and parse the command and process each instruction
   - Detect deadlock for each operation and run the transaction manager according to the parsed operation
2. Transaction Manager
   - A single transaction manager that translates read and write requests on variables to read and write requests on copies using the available copy algorithm
   - Never fails
   - Routes requests and knows the up/down status of each site
   - Each transaction manager has a list of data managers, transaction table, and operation list
   - Detect deadlock using DFS algorithm to find cycle in a blocking graph Data Manager
3. Data Manager
   - Upon recovery of a site, all non-replicated variables are available for reads and writes
   - Replicated variables are available for writing but not reading
   - Read for a replicated variable x will not be allowed at a recovered site until a committed write to x takes place on that site
   - Data manager is initialized one for each site and is responsible for managing locks and data values during transactions
4. Lock Manager
   - Locks a variable x from current site
   - Unlock a variable x from current site
   - Iterate lock on each variable and release it
   - Manage current lock and locks that are added to queue
   - Stores variable id, current lock, and a list of lock in queue
5. Config
   - Configuration file that contains objects, instances, and helper methods
   - Example: Transaction, Operation, Variable etc