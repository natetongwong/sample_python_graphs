from prophecy.config import ConfigBase


class SubgraphConfig(ConfigBase):

    def __init__(
            self,
            prophecy_spark=None,
            output_path: str="dbfs:/data/tmp/customers_merge_python_1",
            merge_condition: str="source.customer_id = target.customer_id",
            **kwargs
    ):
        self.output_path = output_path
        self.merge_condition = merge_condition
        pass

    def update(self, updated_config):
        self.output_path = updated_config.output_path
        self.merge_condition = updated_config.merge_condition
        pass

Config = SubgraphConfig()
