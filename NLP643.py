# Imports the Google Cloud client library
from google.cloud import language
from google.cloud.language import enums
from google.cloud.language import types
import json
# Instantiates a client
client = language.LanguageServiceClient()

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

##print(processed_list)
