from dagster import job, op, graph, Out
import spacy

# Load the SpaCy model
nlp = spacy.load("en_core_web_sm")


# Op to load data
@op
def load_data(context):
    # Simulated dataset containing text data
    data = [
        "John Doe lives in New York.",
        "Jane's email is jane.doe@example.com.",
        "Juudas I think your address is 231, Muranga",
        "She is 34 years old",
    ]
    context.log.info("Original Data:")
    for idx, line in enumerate(data):
        context.log.info(f"Original Line {idx+1}: {line}")
    return data


# Op to apply NER for PII detection
@op(out=Out())
def detect_pii(context, data):
    processed_data = []
    for doc in data:
        ner_doc = nlp(doc)
        redacted_doc = ""
        for token in ner_doc:
            if token.ent_type_ in ["PERSON", "GPE", "EMAIL", "ORG"]:
                redacted_doc += "[REDACTED] "
            else:
                redacted_doc += token.text + " "
        processed_data.append(redacted_doc.strip())

    context.log.info("Processed Data:")
    for idx, line in enumerate(processed_data):
        context.log.info(f"Processed Line {idx+1}: {line}")

    return processed_data


# Op to save or output the processed data
@op(out=Out())
def save_data(context, processed_data):
    for idx, line in enumerate(processed_data):
        context.log.info(f"Final Processed Line {idx+1}: {line}")
    return processed_data


# Define the graph
@graph
def ner_pii_graph():
    raw_data = load_data()
    processed_data = detect_pii(raw_data)
    save_data(processed_data)


# Define the job from the graph
ner_pii_job = ner_pii_graph.to_job()

# Run the job
if __name__ == "__main__":
    result = ner_pii_job.execute_in_process()
