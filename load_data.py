import json
import urllib.request
import pandas as pd

print("מוריד נתוני PubMed...")

# הורדת קובץ ה-JSON הרשמי של PubMedQA
url = "https://raw.githubusercontent.com/pubmedqa/pubmedqa/master/data/ori_pqal.json"
urllib.request.urlretrieve(url, "temp_pubmed.json")

# טעינת הנתונים והמרה למבנה מובנה
with open("temp_pubmed.json", "r", encoding="utf-8") as f:
    data = json.load(f)

records = []
for pmid, content in list(data.items())[:1000]:
    records.append(
        {
            "pmid": pmid,
            "question": content.get("QUESTION", ""),
            "context": " ".join(content.get("CONTEXTS", [])),
            "labels": content.get("LABELS", []),
            "long_answer": content.get("LONG_ANSWER", ""),
            "final_decision": content.get("final_decision", ""),
        }
    )

# שמירה כקובץ JSON lines
df = pd.DataFrame(records)
df.to_json("pubmed_data.json", orient="records", lines=True)

print("הנתונים נשמרו בהצלחה בקובץ pubmed_data.json!")