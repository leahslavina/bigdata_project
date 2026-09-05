import json
from collections import Counter
from pathlib import Path


DATA_FILE = Path("pubmed_data.json")


def load_records(path):
    records = []

    with path.open("r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            line = line.strip()

            if not line:
                continue

            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as error:
                raise ValueError(
                    f"Invalid JSON on line {line_number}: {error}"
                ) from error

    return records


def text_value(value):
    if isinstance(value, list):
        return " ".join(str(part) for part in value)

    return str(value or "")


def main():
    records = load_records(DATA_FILE)
    total = len(records)

    decisions = Counter(
        str(record.get("final_decision") or "unknown").strip().lower()
        for record in records
    )

    context_lengths = [
        len(text_value(record.get("context")).split())
        for record in records
    ]

    records_with_long_answers = sum(
        bool(text_value(record.get("long_answer")).strip())
        for record in records
    )

    average_context_length = (
        sum(context_lengths) / total if total else 0
    )

    print("PUBMED DATASET INSIGHTS")
    print("========================")
    print(f"Total records: {total}")
    print(f"Average context length: {average_context_length:.1f} words")
    print(
        "Records with long answers: "
        f"{records_with_long_answers} "
        f"({records_with_long_answers / total * 100:.1f}%)"
    )

    print("\nFinal-decision distribution:")

    for decision in ("yes", "no", "maybe", "unknown"):
        count = decisions.get(decision, 0)

        if count:
            percentage = count / total * 100
            print(f"- {decision}: {count} ({percentage:.1f}%)")


if __name__ == "__main__":
    main()