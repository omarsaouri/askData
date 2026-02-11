from utils.profiling.constants import SEMANTIC_LABEL_MAP


def extract_semantic_signals(classifier_output: dict):
    semantic_hints = {
        "numeric_value": 0.0,
        "time": 0.0,
        "geographic": 0.0,
        "category_like": 0.0,
        "entity": 0.0,
        "generic_text": 0.0,
        "unknown": 0.0,
    }

    probabilities = classifier_output.get("probabilities", {})

    for label, prob in probabilities.items():
        mapped = SEMANTIC_LABEL_MAP.get(label)
        if mapped:
            semantic_hints[mapped] += prob

    return semantic_hints
