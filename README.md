# Distributed auction system

How to start the system:

Starting replicas:
Open a seperate terminal window in the server folder for each replica and run go run . on them. 
Once a replica has failed, no more replicas should be added.

Starting clients:
open a terminal in the client folder for each client. Run go run . to start the client.
No additional clients should be added once the first three servers are down. 

Using the client:
There are three available commands for the client:
- state         - This command fetches the state of the auction
- bid <amount>  - Attempts to place a bid. Replace <amount> with an integer
- .quit         - Closes the program

Stopping the System:
Press ctrl+C in each terminal to shutdown, or to close the client .quit can also be used. Once all servers are down, bid and state should not be used on a client.

Viewing Logs:
Logs are written to 'server/log\<id\>.txt' and 'client/log\<id\>.txt'
