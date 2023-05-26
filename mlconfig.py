import configparser

class MlConfig:
    def __init__(self):
        configParser = configparser.RawConfigParser() 
        configFilePath = r'config.txt'
        configParser.read(configFilePath)
        self.configParser=configParser

    def getValue(self,section,key):
        return self.configParser.get(section, key)