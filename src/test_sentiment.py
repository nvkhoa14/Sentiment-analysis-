from pyspark.sql import functions as F
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import StringType, StructType, StructField, FloatType
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.ml.feature import Tokenizer, RegexTokenizer
import re
from textblob import TextBlob

spark = SparkSession\
        .builder\
        .appName("TwitterSentimentAnalysis")\
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0")\
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:9.1.7")\
        .getOrCreate()

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "tweet") \
    .load()


spark.sparkContext.setLogLevel("ERROR")


def decode_values(x):
    return x.decode('utf8')

udf_myFunction = udf(decode_values, StringType())
tweet_df = df.withColumn('value', udf_myFunction('value'))

mySchema = StructType([
    StructField("message", StringType(), True)
])
values = tweet_df.select(from_json(tweet_df.value.cast("string"), mySchema).alias("tweet"))

def cleanTweet(tweet: str) -> str:
    tweet = re.sub(r'http\S+', '', str(tweet))
    tweet = re.sub(r'bit.ly/\S+', '', str(tweet))
    tweet = tweet.strip('[link]')

    # remove users
    tweet = re.sub('(RT\s@[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))
    tweet = re.sub('(@[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))

    # remove puntuation
    my_punctuation = '!"$%&\'()*+,-./:;<=>?[\\]^_`{|}~•@â'
    tweet = re.sub('[' + my_punctuation + ']+', ' ', str(tweet))

    # remove number
    tweet = re.sub('([0-9]+)', '', str(tweet))

    # remove hashtag
    tweet = re.sub('(#[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))

    return tweet

df1 = values.select("tweet.*")
clean_tweets = F.udf(cleanTweet, StringType())
raw_tweets = df1.withColumn('processed_text', clean_tweets(col("message")))

# Create a function to get the subjectifvity
def getSubjectivity(tweet: str) -> float:
    return TextBlob(tweet).sentiment.subjectivity


# Create a function to get the polarity
def getPolarity(tweet: str) -> float:
    return TextBlob(tweet).sentiment.polarity


def getSentiment(polarityValue: int) -> str:
    if polarityValue < 0:
        return 'Negative'
    elif polarityValue == 0:
        return 'Neutral'
    else:
        return 'Positive'
    
subjectivity = F.udf(getSubjectivity, FloatType())
polarity = F.udf(getPolarity, FloatType())
sentiment = F.udf(getSentiment, StringType())

subjectivity_tweets = raw_tweets.withColumn('subjectivity', subjectivity(col("processed_text")))
polarity_tweets = subjectivity_tweets.withColumn("polarity", polarity(col("processed_text")))
sentiment_tweets = polarity_tweets.withColumn("sentiment", sentiment(col("polarity")))

# đống ở trên là phân tích ####################################################################################################


 # Start running the query that prints the running counts to the console
query = sentiment_tweets.groupby("sentiment").count() \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()

# CONNECTION_STRING = "mongodb+srv://nvkhoa14:UITHKT@cluster0.irat42q.mongodb.net/lab4.sink"

# def write_row(batch_df , batch_id):
#     df.write.format("mongo").mode("append").option("uri",CONNECTION_STRING).save()
#     # df.write.withWatermark("timestamp", "10 mintutes").groupBy(
#     #     window(df.timestamp, "10 minutes", "5 minutes")
#     # )
#     pass

# # cái này ghi vào mongo
# query = (
#     sentiment_tweets.groupby("sentiment").count().writeStream
#     .foreachBatch(write_row)
#     .start().awaitTermination()
# )

# values.writeStream.format("console").outputMode("append").start().awaitTermination()
