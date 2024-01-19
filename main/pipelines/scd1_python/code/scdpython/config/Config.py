from scdpython.graph.GenerateRandomIncrement.config.Config import SubgraphConfig as GenerateRandomIncrement_Config
from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, GenerateRandomIncrement: dict=None, **kwargs):
        self.spark = None
        self.update(GenerateRandomIncrement)

    def update(self, GenerateRandomIncrement: dict={}, **kwargs):
        prophecy_spark = self.spark
        self.GenerateRandomIncrement = self.get_config_object(
            prophecy_spark, 
            GenerateRandomIncrement_Config(prophecy_spark = prophecy_spark), 
            GenerateRandomIncrement, 
            GenerateRandomIncrement_Config
        )
        pass
