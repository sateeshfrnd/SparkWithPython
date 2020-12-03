class Log4J:

    def __init__(self, spark):
        log4j = spark._jvm.org.apache.log4j
        LOG_ROOT_CLASS = "satish.pysparkapp"
        conf = spark.sparkContext.getConf()
        app_name = conf.get('spark.app.name')
        self.logger = log4j.LogManager.getLogger(f"{LOG_ROOT_CLASS}.{app_name}")

    def wran(self, message):
        self.logger.warn(message)

    def info(self, message):
        self.logger.warn(message)

    def error(self, message):
        self.logger.warn(message)

    def debug(self, message):
        self.logger.warn(message)
