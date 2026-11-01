"""Train intent baseline from a human reviewed CSV: text,label,split."""
import argparse
import csv
import json
from pathlib import Path
import joblib
from sklearn.pipeline import make_pipeline
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report
from .core import normalize


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('csv_path')
    parser.add_argument('--output', default='data/intent.joblib')
    args = parser.parse_args()
    with open(args.csv_path, encoding='utf-8', newline='') as handle:
        rows = list(csv.DictReader(handle))
    splits = {name: [r for r in rows if r['split'] == name] for name in ('train', 'validation', 'test')}
    if any(not group for group in splits.values()):
        raise ValueError('Provide nonempty train, validation and test partitions.')
    if any(r['split'] not in splits for r in rows):
        raise ValueError('Unknown split')
    seen = {}
    for row in rows:
        key = normalize(row['text']).strip()
        if not key or not row['label'].strip():
            raise ValueError('Empty text or label')
        if key in seen and seen[key] != row['split']:
            raise ValueError('Duplicate question across splits')
        seen[key] = row['split']
    labels = {r['label'] for r in splits['train']}
    if len(labels) < 2 or any(r['label'] not in labels for r in rows):
        raise ValueError('Training must contain every label and at least two labels.')
    model = make_pipeline(TfidfVectorizer(preprocessor=normalize, ngram_range=(1, 2)), LogisticRegression(max_iter=1000, random_state=42))
    model.fit([r['text'] for r in splits['train']], [r['label'] for r in splits['train']])
    reports = {}
    for split in ('validation', 'test'):
        group = splits[split]
        reports[split] = classification_report([r['label'] for r in group], model.predict([r['text'] for r in group]), output_dict=True, zero_division=0)
    path = Path(args.output)
    path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, path)
    path.with_suffix('.metrics.json').write_text(json.dumps(reports, indent=2), encoding='utf-8')
    print(json.dumps(reports, indent=2))


if __name__ == '__main__':
    main()
