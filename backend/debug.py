import pandas as pd
from column_classifier import ColumnClassifier

pathfile = "../csvs/clients.csv"
df = pd.read_csv(pathfile)
classifier = ColumnClassifier()

# Check if classify_table exists
print("Has classify_table:", hasattr(classifier, "classify_table"))

# List all methods
print("\nAll methods:")
methods = [method for method in dir(classifier)]

for method in methods:
    print(f"  - {method}")

# Try to call it
try:
    semantic_results = classifier.classify_table(df)
    print("\nSuccess! classify_table works")
    print(semantic_results["columns"])
except AttributeError as e:
    print(f"\nAttributeError: {e}")
except Exception as e:
    print(f"\nOther error: {type(e).__name__}: {e}")
