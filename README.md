# Vietnam SportsBot

Vietnam SportsBot is a Big Data course project developed by Group 4. The project aims to provide a conversational assistant for Vietnamese sports news and events, with support for Vietnamese and English questions.

Users will be able to ask natural language questions, enter short keywords, and receive relevant information with links to the original sources.

## Project Status

The implementation branch includes terminal chat, a Streamlit chat interface, RSS ingestion from VnExpress and Tuoi Tre, SQLite storage, alias normalization, TF IDF retrieval and optional grounded responses through a configured language model API.

Without API credentials, the application displays labeled source excerpts. The language model path requires a user supplied provider configuration and has not been validated against a live provider in this environment.

The training module supports a separate supervised intent classification experiment using a user supplied labeled dataset. No trained classifier or measured model quality is claimed. The classifier is not yet connected to answer routing.

RSS coverage is limited. Structured match APIs, comprehensive historical retrieval, strict event time filtering, semantic embeddings and HDFS or Spark jobs remain future work. Source ID validation checks citation format but cannot prove that every generated claim is supported. Human evaluation remains necessary.

See [Setup and Operation](docs/SETUP.md) for executable installation instructions and [Training Guide](docs/TRAINING.md) for the supervised learning workflow.

## Problem Statement

Sports information is distributed across news websites, competition pages, and data services. Users may describe the same team or event using different names, abbreviations, or languages.

The project addresses the challenge of collecting this information, maintaining its freshness, retrieving relevant evidence, and presenting an answer that users can verify.

A question about the result of a Vietnam versus Indonesia match may require additional context, such as the date, competition, age group, or team category. The assistant should identify ambiguity and request clarification when necessary.

## Objectives

1. Collect sports news and event information from selected RSS feeds, permitted website pages, and sports APIs.
2. Preserve source URLs, publication times, collection times, and data provenance.
3. Clean collected records and prevent duplicate storage.
4. Support Vietnamese and English questions and keywords.
5. Retrieve relevant news and structured event records.
6. Generate answers supported by identifiable sources.
7. Provide terminal and browser interfaces that share the same processing logic.
8. Apply HDFS and Spark to data storage, processing, and analysis.
9. Evaluate retrieval quality, answer grounding, freshness, and response time.

## Initial Scope

The initial target is Vietnamese football and volleyball, including domestic competitions and Vietnamese teams participating in international events.

Vietnamese and English are the initial supported language targets. Additional sports, sources, and languages may be introduced after the core workflow has been evaluated.

Coverage depends on the connected sources and their available historical data. The project does not claim complete coverage of every sports website or event.

## Intended User Experience

Users may search for a team, competition, athlete, or event using a short phrase or a conversational question.

The assistant should distinguish between news requests, match result questions, schedule questions, and standings questions.

For event results, the response should identify the teams or participants, competition, event date, verified result, and supporting source. The system should verify the result even when the question assumes a particular team won.

If several events match a question, the assistant should ask for clarification or clearly identify which event the answer describes. If evidence is insufficient, it should state that limitation.

## Proposed Architecture

1. Source adapters collect records from configured RSS feeds and APIs.
2. Raw storage preserves the collected data and provenance.
3. Processing validates fields, normalizes timestamps, and removes duplicates.
4. Curated storage contains records suitable for retrieval and analysis.
5. Query processing identifies entities, intent, and time constraints.
6. Retrieval selects relevant articles or structured event records.
7. The language model generates a response using the retrieved evidence.
8. The interface presents the answer with source links and timestamps.

The terminal interface and Streamlit interface will use shared application services.

## Data and Big Data Processing

SQLite is the initial local storage choice for the application prototype.

HDFS is planned for storing raw and curated datasets. Spark is planned for cleaning, deduplication, transformation, and reporting. Curated datasets may be stored in Parquet format and exported into the application retrieval store.

Processing reports should record input counts, output counts, rejected records, duplicate counts, execution times, and known limitations.

The local Hadoop environment represents a teaching setup. Running services on one computer does not demonstrate the capacity of a production distributed cluster.

## NLP and Language Models

The primary question answering approach is Retrieval Augmented Generation, or RAG. New information is added to the retrieval store rather than requiring the language model to be trained again after every event.

Planned NLP tasks include entity normalization, abbreviation handling, intent classification, time expression interpretation, and multilingual semantic retrieval.

An optional supervised component is an intent classifier trained on labeled Vietnamese and English questions. A baseline may use TF IDF features and Logistic Regression. Training, validation, and test data should be separated, with similar questions kept from leaking across splits.

Training a foundation language model from scratch is outside the intended scope. LLM fine tuning is not a prerequisite for the initial application.

## Planned Technology Stack

1. Ubuntu 24.04 through WSL2 for the development environment.
2. Python 3.12 for application development.
3. Streamlit for the browser interface.
4. Python argparse and Rich for the terminal interface.
5. Feedparser and HTTPX for data collection.
6. Beautiful Soup for permitted HTML extraction.
7. SQLite for initial application storage.
8. Scikit learn for baseline retrieval and classification.
9. Multilingual embeddings for semantic retrieval.
10. An LLM API for evidence based answer generation.
11. Hadoop HDFS and Apache Spark for Big Data workflows.
12. Git and GitHub for version control and collaboration.

Exact application dependencies will be documented when the source code is published.

## Development Roadmap

1. Publish the existing local prototype and dependency file.
2. Connect verified RSS sources and preserve raw records.
3. Add SQLite storage and duplicate prevention.
4. Replace synthetic records with collected articles.
5. Add structured event data for results and schedules.
6. Improve entity normalization and query interpretation.
7. Add multilingual retrieval and grounded LLM responses.
8. Integrate HDFS and Spark processing.
9. Train and evaluate the optional intent classifier.
10. Deploy the browser interface and document the demonstration workflow.

## Evaluation

Evaluation will include relevant article retrieval, correct event identification, accurate source attribution, handling of ambiguous questions, multilingual query performance, collection delay, and response time.

The application should also be evaluated when no relevant data exists, when a source becomes unavailable, and when collection is repeated.

Measured results and reproducible run records will be added as development progresses.

## Deployment Plan

The terminal application is intended for technical demonstrations. The Streamlit application is intended for browser access and classroom testing.

GitHub will store source code and documentation. Streamlit Community Cloud is the initial candidate for hosting the browser interface.

Continuous collection requires a running collector and persistent storage. Publishing a web interface alone does not guarantee continuous collection or durable data storage.

## Configuration and Data Handling

API credentials must be stored outside version control using environment variables or deployment secrets.

The repository should exclude virtual environments, private credentials, local databases, and collected data archives. Source selection and collection behavior must respect applicable access conditions and usage limits.

Synthetic records must remain clearly labeled. Generated answers should preserve the distinction between article publication time, event time, and collection time.

## Academic Context

This project is developed for a Big Data course. The final report should explain the architecture, data provenance, processing decisions, evaluation results, failure cases, and limitations.

Any use of AI assistance in implementation or reporting should be documented according to the course requirements.
