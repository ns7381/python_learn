class Source:

    def __init__(self, file_path, source_type, options, schema_json):
        self.file_path = file_path
        self.source_type = source_type
        self.options = options
        self.schema_json = schema_json
    #
    # @property
    # def schema_json(self):
    #     return self._schema_json
    #
    # @schema_json.setter
    # def schema_json(self, value):
    #     self._schema_json = value


class Training:

    def __init__(self, algorithm_type, algorithm, params, training_rate, source, model_path):
        self.algorithm_type = algorithm_type
        self.algorithm = algorithm
        self.params = params
        self.training_rate = training_rate
        self.source = source
        self.model_path = model_path
