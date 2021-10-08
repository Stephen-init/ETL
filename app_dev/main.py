from clean import *

if __name__ == '__main__':
    config = luigi.configuration.get_config()
    config.read('luigi.cfg')
    luigi.build([GenerateExceptionReports()], workers=10)