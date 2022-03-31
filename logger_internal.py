import logging

logFormatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
rootLogger = logging.getLogger("app")
rootLogger.setLevel(logging.INFO)

fileHandler = logging.FileHandler("spark_test.log", mode="w")
fileHandler.setLevel(logging.DEBUG)
fileHandler.setFormatter(logFormatter)
rootLogger.addHandler(fileHandler)

consoleHandler = logging.StreamHandler()
consoleHandler.setLevel(logging.DEBUG)
consoleHandler.setFormatter(logFormatter)
rootLogger.addHandler(consoleHandler)
