import json
import re
import time
import warnings
from typing import Any, Dict, List, Optional

import spacy

# Suppress all warnings coming from spaCy/pydantic noise in CLI contexts
warnings.filterwarnings("ignore")


class ColumnClassifier:
    """Classify DataFrame columns using spaCy with optional LLM assistance."""

    VALID_CLASSES = [
        "PERSON",
        "LOCATION",
        "ORGANIZATION",
        "NUMBER",
        "DATE",
        "STRING",
        "OTHER",
    ]

    LITERAL_TYPES = {"NUMBER", "DATE", "STRING"}
    ENTITY_TYPES = {"PERSON", "LOCATION", "ORGANIZATION", "OTHER"}

    DEFAULT_SYSTEM_PROMPT = (
        "You are an assistant that classifies tabular data columns. "
        "Choose the best label among PERSON, LOCATION, ORGANIZATION, NUMBER, DATE, STRING, or OTHER."
    )

    def __init__(
        self,
        sample_size: int = 50,
        classification_threshold: float = 0.5,
        word_threshold: int = 10,
        llm_config: Optional[Dict[str, Any]] = None,
        llm_weight: float = 0.5,
        separator: str = " | ",
        random_state: int = 1,
        spacy_batch_size: int = 1024,
        spacy_model: str = "en_core_web_sm",
    ) -> None:
        self.sample_size = sample_size
        self.classification_threshold = classification_threshold
        self.word_threshold = word_threshold
        self.separator = separator
        self.random_state = random_state
        self.spacy_batch_size = spacy_batch_size
        self.spacy_model = spacy_model

        self.nlp = self._load_spacy_model()

        self.llm_config = llm_config or {}
        self.llm_weight = min(max(llm_weight, 0.0), 1.0)
        self.llm_client = None
        self.llm_model = None
        self.llm_system_prompt = self.llm_config.get(
            "system_prompt", self.DEFAULT_SYSTEM_PROMPT
        )
        try:
            self.llm_max_retries = max(0, int(self.llm_config.get("max_retries", 2)))
        except (TypeError, ValueError):
            self.llm_max_retries = 2
        try:
            self.llm_retry_delay = max(
                0.0, float(self.llm_config.get("retry_delay", 0.0))
            )
        except (TypeError, ValueError):
            self.llm_retry_delay = 0.0

        if self.llm_config.get("enabled"):
            self._setup_llm_client()

        # Map spaCy fine-grained labels to high-level types
        self.label_map = {
            "DATE": "DATE",
            "TIME": "DATE",
            "MONEY": "NUMBER",
            "PERCENT": "NUMBER",
            "QUANTITY": "NUMBER",
            "CARDINAL": "NUMBER",
            "ORDINAL": "NUMBER",
            "GPE": "LOCATION",
            "LOC": "LOCATION",
            "ORG": "ORGANIZATION",
            "PERSON": "PERSON",
            "WORK_OF_ART": "OTHER",
            "EVENT": "OTHER",
            "FAC": "OTHER",
            "PRODUCT": "OTHER",
            "LAW": "OTHER",
            "NORP": "OTHER",
            "LANGUAGE": "OTHER",
        }

    def _load_spacy_model(self):
        try:
            return spacy.load(self.spacy_model)
        except OSError as exc:
            raise OSError(
                f"spaCy model '{self.spacy_model}' is not installed. "
                f"Install it with `python -m spacy download {self.spacy_model}`."
            ) from exc

    def _setup_llm_client(self) -> None:
        try:
            from ollama import Client as OllamaClient
        except ImportError as exc:
            raise ImportError(
                "LLM support requires the optional dependency `ollama`. "
                "Install it with `pip install ollama`."
            ) from exc

        self.llm_model = self.llm_config.get("model")
        if not self.llm_model:
            raise ValueError("llm_config must include a 'model' when enabled is True.")

        host = self.llm_config.get("host")
        headers = self.llm_config.get("headers")

        self.llm_client = OllamaClient(host=host, headers=headers)

    def map_spacy_label(self, label: str) -> str:
        return self.label_map.get(label, "OTHER")

    @staticmethod
    def _is_number(value: str) -> bool:
        try:
            float(value)
            return True
        except ValueError:
            return False

    def classify_text_batch(
        self, texts: List[str], sample_data_list: List[Any]
    ) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []

        for doc, sample_data in zip(
            self.nlp.pipe(texts, batch_size=self.spacy_batch_size), sample_data_list
        ):
            sample_series = sample_data.astype(str)
            num_rows = len(sample_series)
            if num_rows == 0:
                results.append(
                    {"probabilities": {"STRING": 1.0}, "avg_word_count": 0.0}
                )
                continue

            entity_counts = {
                "LOCATION": 0,
                "ORGANIZATION": 0,
                "PERSON": 0,
                "OTHER": 0,
            }
            literal_counts = {"NUMBER": 0, "DATE": 0, "STRING": 0}

            for ent in doc.ents:
                high_level_class = self.map_spacy_label(ent.label_)
                if high_level_class in literal_counts:
                    literal_counts[high_level_class] += 1
                else:
                    entity_counts[high_level_class] += 1

            number_count = int(sample_series.apply(self._is_number).sum())
            literal_counts["NUMBER"] = min(number_count, num_rows)

            literal_counts["STRING"] += max(0, num_rows - len(doc.ents) - number_count)

            probabilities: Dict[str, float] = {}
            combined_counts = {**entity_counts, **literal_counts}
            for key, count in combined_counts.items():
                if count > 0:
                    probabilities[key] = min(count / num_rows, 1.0)

            if not probabilities:
                probabilities = {"STRING": 1.0}

            total_words = sum(len(str(x).split()) for x in sample_series)
            avg_word_count = total_words / num_rows if num_rows else 0.0

            results.append(
                {
                    "probabilities": self._normalize_probabilities(probabilities),
                    "avg_word_count": avg_word_count,
                }
            )

        return results

    def classify_column(
        self, column_data, column_name: str = "column", separator: Optional[str] = None
    ) -> Dict[str, Any]:
        sep = separator or self.separator
        non_na_data = column_data.dropna().astype(str)
        if non_na_data.empty:
            return {"classification": "STRING", "probabilities": {"STRING": 1.0}}

        sample_size = min(self.sample_size, len(non_na_data))
        sample_data = (
            non_na_data.sample(n=sample_size, random_state=self.random_state)
            .reset_index(drop=True)
        )
        concatenated_text = sep.join(sample_data.tolist())

        [col_info] = self.classify_text_batch([concatenated_text], [sample_data])
        return self._finalize_result(column_name, sample_data, col_info)

    def classify_table(
        self,
        table,
        table_name: str = "table",
        columns: Optional[List[str]] = None,
        separator: Optional[str] = None,
    ) -> Dict[str, Any]:
        sep = separator or self.separator
        selected_columns = columns or list(table.columns)

        texts: List[str] = []
        sample_data_list: List[Any] = []
        column_order: List[str] = []

        for column in selected_columns:
            if column not in table.columns:
                continue
            non_na_data = table[column].dropna().astype(str)
            if non_na_data.empty:
                continue

            sample_size = min(self.sample_size, len(non_na_data))
            sample_data = (
                non_na_data.sample(n=sample_size, random_state=self.random_state)
                .reset_index(drop=True)
            )
            concatenated_text = sep.join(sample_data.tolist())

            texts.append(concatenated_text)
            sample_data_list.append(sample_data)
            column_order.append(column)

        if not texts:
            return {"table": table_name, "columns": {}}

        spacy_outputs = self.classify_text_batch(texts, sample_data_list)

        column_results: Dict[str, Dict[str, Any]] = {}
        for column, col_info, sample_data in zip(
            column_order, spacy_outputs, sample_data_list
        ):
            column_results[column] = self._finalize_result(
                column, sample_data, col_info
            )

        return {"table": table_name, "columns": column_results}

    def classify_multiple_tables(
        self,
        tables: List[Any],
        separator: Optional[str] = None,
        table_names: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        for idx, table in enumerate(tables):
            name = (
                table_names[idx]
                if table_names and idx < len(table_names)
                else f"table_{idx + 1}"
            )
            results.append(
                self.classify_table(table, table_name=name, separator=separator)
            )
        return results

    def _finalize_result(self, column_name: str, sample_data, col_info: Dict[str, Any]):
        baseline_probabilities = dict(col_info["probabilities"])
        probabilities = dict(baseline_probabilities)
        avg_word_count = col_info["avg_word_count"]

        llm_response = self._maybe_call_llm(column_name, sample_data, probabilities)
        if llm_response and llm_response.get("source") != "fallback":
            probabilities = self._merge_probabilities(
                probabilities, llm_response.get("probabilities", {})
            )

        classification = self._select_classification(probabilities, avg_word_count)

        if llm_response:
            llm_class = llm_response.get("classification")
            llm_probs = llm_response.get("probabilities", {})
            if llm_response.get("source") == "fallback":
                classification = llm_class or classification
            elif llm_class and llm_probs.get(llm_class, 0) >= self.classification_threshold:
                classification = llm_class

        normalized_probabilities = self._normalize_probabilities(probabilities)
        if classification not in normalized_probabilities:
            normalized_probabilities[classification] = 1.0
            normalized_probabilities = self._normalize_probabilities(normalized_probabilities)

        sources: Dict[str, Any] = {
            "spacy": {
                "probabilities": self._normalize_probabilities(baseline_probabilities),
                "avg_word_count": avg_word_count,
            }
        }
        if llm_response:
            sources["llm"] = {
                "source": llm_response.get("source"),
                "classification": llm_response.get("classification"),
                "probabilities": self._normalize_probabilities(
                    llm_response.get("probabilities", {})
                ),
                "attempt": llm_response.get("attempt"),
            }

        return {
            "classification": classification,
            "probabilities": normalized_probabilities,
            "sources": sources,
        }

    def _select_classification(
        self, probabilities: Dict[str, float], avg_word_count: float
    ) -> str:
        if avg_word_count > self.word_threshold:
            return "STRING"

        ne_probs = {k: probabilities.get(k, 0.0) for k in self.ENTITY_TYPES}
        lit_probs = {k: probabilities.get(k, 0.0) for k in self.LITERAL_TYPES}

        if ne_probs:
            max_ne_type = max(ne_probs, key=ne_probs.get)
            if ne_probs[max_ne_type] >= self.classification_threshold:
                return max_ne_type

        if lit_probs:
            max_lit_type = max(lit_probs, key=lit_probs.get)
            if lit_probs[max_lit_type] >= self.classification_threshold:
                return max_lit_type

        if sum(ne_probs.values()) >= self.classification_threshold:
            return "OTHER"

        if probabilities:
            return max(probabilities, key=probabilities.get)

        return "STRING"

    def _normalize_probabilities(
        self, probabilities: Dict[str, float]
    ) -> Dict[str, float]:
        cleaned: Dict[str, float] = {}
        for label, value in probabilities.items():
            if label not in self.VALID_CLASSES:
                continue
            try:
                numeric = float(value)
            except (TypeError, ValueError):
                continue
            numeric = min(max(numeric, 0.0), 1.0)
            if numeric > 0:
                cleaned[label] = round(numeric, 2)

        if not cleaned:
            return {"STRING": 1.0}

        return cleaned

    def _maybe_call_llm(self, column_name, sample_data, spacy_probabilities):
        if not self.llm_client:
            return None

        prompt = self._build_llm_prompt(column_name, sample_data, spacy_probabilities)
        messages = self._build_llm_messages(prompt)

        attempts = self.llm_max_retries + 1
        for attempt in range(attempts):
            try:
                response = self.llm_client.chat(
                    model=self.llm_model,
                    messages=messages,
                )
            except Exception as exc:  # pragma: no cover - runtime warning only
                warnings.warn(f"LLM classification failed for '{column_name}': {exc}")
                break

            parsed = self._parse_llm_response(response)
            if parsed:
                parsed.setdefault("source", "llm")
                parsed["attempt"] = attempt + 1
                return parsed

            content = self._extract_llm_content(response)
            heuristic = self._extract_label_from_text(content)
            if heuristic:
                heuristic["attempt"] = attempt + 1
                return heuristic

            if attempt < attempts - 1:
                messages = self._reinforce_llm_messages(messages, attempt + 1)
                if self.llm_retry_delay:
                    time.sleep(self.llm_retry_delay)

        fallback_label = (
            max(spacy_probabilities, key=spacy_probabilities.get)
            if spacy_probabilities
            else "STRING"
        )
        warnings.warn(
            f"LLM output not usable for '{column_name}'. Falling back to baseline label '{fallback_label}'."
        )
        return {
            "classification": fallback_label,
            "probabilities": {fallback_label: 1.0},
            "source": "fallback",
            "attempt": attempts,
        }

    def _build_llm_prompt(
        self, column_name, sample_data, spacy_probabilities: Dict[str, float]
    ) -> str:
        max_samples = int(self.llm_config.get("max_samples", 10))
        preview = sample_data.iloc[:max_samples].tolist()
        sample_lines = "\n".join(
            f"{idx + 1}. {value}" for idx, value in enumerate(preview)
        )
        spacy_summary = ", ".join(
            f"{label}: {prob:.2f}" for label, prob in spacy_probabilities.items()
        )

        return (
            "Classify the following column using the label set "
            "PERSON, LOCATION, ORGANIZATION, NUMBER, DATE, STRING, OTHER.\n"
            f"Column name: {column_name}\n"
            "Sample values:\n"
            f"{sample_lines}\n\n"
            f"spaCy baseline probabilities (for reference only): {spacy_summary or 'none'}\n\n"
            "Respond with JSON only using the schema:\n"
            '{"classification": "<label>", "probabilities": {"LABEL": float, ...}}\n'
            "Probabilities must be between 0 and 1 and sum to 1. Derive them from the "
            "samples yourself; do not simply repeat the baseline."
        )

    def _build_llm_messages(self, prompt: str) -> List[Dict[str, str]]:
        messages: List[Dict[str, str]] = [{"role": "user", "content": prompt}]
        if self.llm_system_prompt:
            messages.insert(0, {"role": "system", "content": self.llm_system_prompt})
        return messages

    def _reinforce_llm_messages(
        self, messages: List[Dict[str, str]], attempt: int
    ) -> List[Dict[str, str]]:
        reinforcement = (
            "Reminder: respond with JSON only using the schema "
            '{"classification": "<label>", "probabilities": {"LABEL": float, ...}}.'
        )
        updated = list(messages)
        updated.append(
            {
                "role": "user",
                "content": f"{reinforcement} Attempt #{attempt + 1}.",
            }
        )
        return updated

    def _extract_llm_content(self, response: Any) -> str:
        if isinstance(response, dict):
            message = response.get("message")
            if isinstance(message, dict):
                content = message.get("content")
                if isinstance(content, str):
                    return content.strip()
            content = response.get("content")
            if isinstance(content, str):
                return content.strip()
            return ""

        message_obj = getattr(response, "message", None)
        if message_obj is not None:
            content = getattr(message_obj, "content", None)
            if isinstance(content, str):
                return content.strip()

        direct_content = getattr(response, "content", None)
        if isinstance(direct_content, str):
            return direct_content.strip()

        if isinstance(response, str):
            return response.strip()

        return ""

    def _parse_llm_response(self, response: Any):
        content = self._extract_llm_content(response)
        if not content:
            return None

        json_block = self._extract_json_block(content)
        if not json_block:
            return None

        try:
            parsed = json.loads(json_block)
        except json.JSONDecodeError:
            return None

        classification = parsed.get("classification")
        if isinstance(classification, str):
            classification = classification.strip().upper()
        probabilities = parsed.get("probabilities", {}) or {}

        normalized = self._normalize_probabilities(probabilities)
        if classification not in self.VALID_CLASSES:
            classification = None

        return {
            "classification": classification,
            "probabilities": normalized,
            "source": "llm",
        }

    def _extract_json_block(self, content: str) -> Optional[str]:
        fenced_match = re.search(
            r"```(?:json)?\s*(\{.*?\})\s*```", content, flags=re.DOTALL | re.IGNORECASE
        )
        if fenced_match:
            return fenced_match.group(1)

        start = content.find("{")
        end = content.rfind("}")
        if start == -1 or end == -1 or start >= end:
            return None
        return content[start : end + 1]

    def _extract_label_from_text(self, content: str) -> Optional[Dict[str, Any]]:
        if not content:
            return None

        match = re.search(
            r"classification\s*[:=]\s*([A-Za-z_]+)", content, flags=re.IGNORECASE
        )
        candidate = match.group(1).strip().upper() if match else ""
        if candidate in self.VALID_CLASSES:
            return {
                "classification": candidate,
                "probabilities": {candidate: 1.0},
                "source": "heuristic",
            }

        upper_content = content.upper()
        for label in self.VALID_CLASSES:
            token = f" {label} "
            if token in f" {upper_content} ":
                return {
                    "classification": label,
                    "probabilities": {label: 1.0},
                    "source": "heuristic",
                }

        return None

    def _merge_probabilities(
        self, spacy_probabilities: Dict[str, float], llm_probabilities: Dict[str, float]
    ) -> Dict[str, float]:
        if not llm_probabilities:
            return spacy_probabilities

        combined: Dict[str, float] = {}
        spacy_weight = 1.0 - self.llm_weight if llm_probabilities else 1.0
        llm_weight = self.llm_weight if llm_probabilities else 0.0

        for label in set(spacy_probabilities) | set(llm_probabilities):
            total = 0.0
            weight_sum = 0.0
            if label in spacy_probabilities and spacy_weight > 0:
                total += spacy_probabilities[label] * spacy_weight
                weight_sum += spacy_weight
            if label in llm_probabilities and llm_weight > 0:
                total += llm_probabilities[label] * llm_weight
                weight_sum += llm_weight
            if weight_sum == 0:
                continue
            combined[label] = total / weight_sum

        return combined
