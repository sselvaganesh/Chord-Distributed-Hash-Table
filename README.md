# cs457-cs557-pa1-ssudala1

----------------------------------------------------------

Programming Language Opted: GO
Total files: 1
File name: server.go

----------------------------------------------------------

To compile the program:
-----------------------

Using Bash:
-----------

	1. Set the GOPATH environment variable as below
		1.1 Set the current working directory to "src" folder
		1.2 Execute the below command in bash
			export GOPATH=`pwd`

	2. Get the git.apache.org/thrift 
		2.1 Execute command "go get git.apache.org/thrift.git/lib/go/thrift"

	2. Create an executable for server.go program
		2.1 Set the current working directory to "src>server" folder
		2.2 Execute the below command
			go build .


Using makefile:
--------------

	1. Execute the below command in the terminal
		1.1 Set the current working to to "src/server"
		1.2 type "go build" and ENTER



Run the program:
----------------

3. Run the program by executing server executable
	3.1 Execute the below command in bash
		./server <PortNumber>


----------------------------------------------------------

Implementation:
---------------

Implemented Chord DHT as mentioned in the assignment description

	Overview:
	--------
	1. Tested "SetFingerTable" method using init program gives as part of the assignment
	2. Created "Client.go" to test the other function as part of the chord DHT
	3. "GetNodeSucc" function successfully returns the very first entry in the finger table initialized for the node
	3. "FindPred" function, takes the key value received as part of the input and checks each value 
	(First entry first from the finger table, then process from the last entry) from the finger table
	4. If the key value matches between current node and the finger table entry, it returns the current node as the predecessor
	5. "FindSucc" will correctly identify the successor of the currnt node and sends back the NodeID back to the client.
	6. "WriteFile" receives the RFile value from the client and checks the current node is the successor, it will store the file
	conrent, version, hashvalue of the file with it.
	7. "ReadFile" receives the file name from the client and checks the current node as the owner of the file. If yes, it will
	retrieve the content and send back the result to cliet.

----------------------------------------------------------
