from google.cloud import bigquery
from google.cloud import language
from google.cloud.language import enums
from google.cloud.language import types
import json
# Instantiates clients
client = language.LanguageServiceClient()
client1 = bigquery.Client()

def NLPGaugeSentiment(review_obj):
    list = []
    text = review_obj['Review']
    quality = review_obj['Quality']
    difficulty = review_obj['Difficulty']
    document = types.Document(
        content=text,
        type=enums.Document.Type.PLAIN_TEXT)

    # Detects the entity sentiment of the text
    response = client.analyze_sentiment(document, encoding_type='UTF8')

    # Return response
    return_data = {}

    return_data['review'] = text
    return_data['sentiment_score'] = response.document_sentiment.score
    return_data['sentiment_magnitude'] = response.document_sentiment.magnitude
    return_data['quality'] = quality
    return_data['difficulty'] = difficulty

    list.append(return_data)
    return (list)

processed_list = []
with open('rmp-borcea1.json') as json_file:
    review_list = json.load(json_file)
    for review in review_list:
        processed_list.extend(NLPGaugeSentiment(review))
print(json.dumps(processed_list))

dataset_ref = client.dataset(json_file)
job_config = bigquery.LoadJobConfig()
job_config.schema = [
    bigquery.SchemaField("difficulty", "FLOAT"),
    bigquery.SchemaField("quality", "FLOAT"),
    bigquery.SchemaField("sentiment_magnitude", "FLOAT"),
    bigquery.SchemaField("sentiment_score", "FLOAT"),
    bigquery.SchemaField("review", "STRING"),
]
job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON

load_job = client.load_table(
    dataset_ref.table("rmpReview"),
    location="US",  # Location must match that of the destination dataset.
    job_config=job_config,
)  # API request
print("Starting job {}".format(load_job.job_id))

load_job.result()  # Waits for table load to complete.
print("Job finished.")

destination_table = client.get_table(dataset_ref.table("rmpReview"))
print("Loaded {} rows.".format(destination_table.num_rows))
