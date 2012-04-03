Anyone changing the ZooKeeper client for ExactTarget needs to be sure to:

1. Increment the version number of the ZooKeeperNet.dll assembly
2. Copy the updated DLL to the ExactTargetDev/Kafka lib directory and the Common/Zookeeper directory in the internal ExactTarget repo

To build the .NET dlls you need to have ANT installed along with Java in your path. Run ant from the bash command. This will generate required .cs files. Next open the solution file and use MSBUILD or open in VS and compile. When compiling to check in dlls in other repositories, make sure it is build in RELEASE mode.
