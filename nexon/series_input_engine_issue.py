import numpy as np
from razor import Block, inputs, outputs, Pipeline

@inputs.atomic.generic(name="load_path")
@outputs.series.generic("data")
class DataLoader(Block):
    def run(self, load_path, data):
        for i in range(4):
            data.put({"input_buffer": np.ones([298, 298, 3]).astype(float), "label_buffer": np.ones(1).astype(np.float32)})



@inputs.series.generic("data")
@inputs.atomic.generic("path")
class CSVWriter(Block):
    def run(self, path, data):
        import os
        import pandas as pd
        if os.path.exists(project_space_path(path)):
            os.remove(project_space_path(path))
        for batch in data:
        	pass

train_loader = DataLoader()
csv_writer = CSVWriter("hima_csv_writer").path("hub_engine.csv").data(train_loader.data)

pipeline = Pipeline(targets=[csv_writer], name="hima_pipeline")
pipeline.show()

import razor
deployed_pipeline= razor.platform.engines(name='Engine-1-dev1').execute(pipeline=pipeline)

deployed_pipeline.monitor()

deployed_pipeline.metrics().plot()
