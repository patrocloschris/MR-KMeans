import getopt
import os.path
import sys



def checkInputArguments(logger,argv,hadoopHome,inputFile,outputFile,numberOfExecutions,numberOfClusters):

    argumentErrorMsg = "executor.py -d <hadoopHome> -i <inputfile> -c <executionTimes> -o <outputfile> -n <numberOfClusters>"

    # read input parameters
    try:
        opts, args = getopt.getopt(argv, "d:i:c:o:n:")
    except getopt.GetoptError:
        logger.error(argumentErrorMsg)
        sys.exit(2)

    for opt, arg in opts:
        if opt == '-h':
            logger.error(argumentErrorMsg)
            sys.exit(0)
        elif opt in "-d":
            hadoopHome = arg
        elif opt in "-i":
            inputFile = arg
        elif opt in "-o":
            outputFile = arg
        elif opt in "-c":
            numberOfExecutions = int(arg)
        elif opt in "-n":
            numberOfClusters = int(arg)

    # check if that files exist in the os
    logger.info("Checking input parameters")
    if not hadoopHome.endswith('/'):
        hadoopHome = hadoopHome + "/"
    hadoopJar = hadoopHome + "bin/hadoop"

    if os.path.isfile(hadoopJar) == False:
        logger.error("File %s does not exist", hadoopJar)
        sys.exit()

    if isinstance(numberOfExecutions, int):
        logger.error("executionTimes parameter is not an integer")
        sys.exit()
    else:
        numberOfExecutions = int(numberOfExecutions)
        if numberOfExecutions < 1:
            logger.error("executionTimes parameter must be 1 or more")
            sys.exit()

    if isinstance(numberOfClusters, int):
        logger.error("numberOfClusters parameter is not an integer")
        sys.exit()
    else:
        numberOfClusters = int(numberOfClusters)
        if numberOfClusters < 1:
            logger.error("numberOfClusters parameter must be 1 or more")
            sys.exit()