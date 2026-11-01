# Training Guide

The application uses retrieval and a configured language model. It does not train or fine tune an LLM during collection.

The optional intent classifier is a separate academic experiment. It is not yet used for routing production answers.

Prepare a CSV with columns text,label,split. Suggested labels are match_result, match_schedule, sports_news and standings. Include Vietnamese and English examples, short keywords, abbreviations and ambiguous questions. Have humans review every label. Aim initially for at least 100 diverse examples per label and language, then inspect learning curves rather than assuming that quantity is sufficient.

Assign question families to train, validation or test before paraphrasing. Keep all paraphrases of one question in the same partition. The script rejects normalized exact duplicates across partitions but cannot detect semantic duplicates automatically.

Run the module sportsbot.train with the CSV path as its positional argument. It trains TF IDF and Logistic Regression, saves a model and produces validation and test classification reports. Use validation for development. Reserve the final test set until decisions are frozen. Report class counts, macro F1, per class precision and recall, language specific evaluation and error examples. Do not claim that the model has been trained before supplying a dataset and running the procedure.

A future routing integration must include confidence calibration and an unknown intent fallback. Intent classification does not establish factual match results.
