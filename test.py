import pandas as pd
import json
from csv_schema_inference import csv_schema_inference
from csv_detective import routine
from column_classifier import ColumnClassifier


pathfile = "./csvs/clients.csv"

df = pd.read_csv(pathfile)


classifier = ColumnClassifier()
semantic_results = classifier.classify_table(df)
print(semantic_results["columns"])

if __name__ == "__main__":
    csv_infer = csv_schema_inference.CsvSchemaInference(
        portion=0.9,
        max_length=100,
        batch_size=200000,
        acc=0.8,
        seed=2,
        header=True,
        sep=",",
    )

    df = pd.read_csv(pathfile)

    # Run inference
    schema = csv_infer.run_inference(pathfile)
    # csv_infer.pretty(schema)

    inspection_results = routine(
        pathfile,  # or file URL
        num_rows=-1,  # Value -1 will analyze all lines of your file, you can change with the number of lines you wish to analyze
        save_results=False,  # Default False. If True, it will save result output into the same directory as the analyzed file, using the same name as your file and .json extension
        output_profile=True,  # Default False. If True, returned dict will contain a property "profile" indicating profile (min, max, mean, tops...) of every column of your csv
        output_schema=True,  # Default False. If True, returned dict will contain a property "schema" containing basic [tableschema](https://specs.frictionlessdata.io/table-schema/) of your file. This can be used to validate structure of other csv which should match same structure.
        tags=[
            "fr"
        ],  # Default None. If set as a list of strings, only performs checks related to the specified tags (you can see the available tags with FormatsManager().available_tags())
    )
