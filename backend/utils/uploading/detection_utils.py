import chardet
import csv


def detect_encoding(contents: bytes):
    return chardet.detect(contents)["encoding"] or "utf-8"

def detect_delimiter(sample: str):
    return csv.Sniffer().sniff(sample).delimiter