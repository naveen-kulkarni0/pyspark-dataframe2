from pyspark.sql import SparkSession

spark = SparkSession \
        .builder \
        .appName("Spark Sql basic") \
        .getOrCreate()


source_path = "./carnivorous_diet.csv"
carnivorous_df = spark.read.csv(source_path, header = True)
#describes a particular key with various aspects such as count, mean etc
print(carnivorous_df.describe("familyCarni").show())

print("###########################################")

print(carnivorous_df.count())

print("###########################################")

# To print all unique values in ascending order for a key
carnivorous_df.select("commonNameCarni").drop_duplicates().sort("commonNameCarni").show()

print("###########################################")

#We can also select few coloums with a condition like below
filtered_result = carnivorous_df.select(["recordID","commonNameCarni","foodType"]).where(carnivorous_df.commonNameCarni == "African golden cat").limit(2).collect()
print(filtered_result)

print("###########################################")

#We can also drop records which doesn't have certian mandatory fields
carnivorous_valid_df = carnivorous_df.dropna(how="any", subset=["recordID","orderCarni","familyCarni","scientificNameCarni","commonNameCarni","sexCarni","lifeStageCarni","foodType","orderPrey","familyPrey","genusPrey","speciesPrey","scientificNamePrey","commonNamePrey","domesticOrAgricultural","taxonRankPrey","percentage","percentageError","dataBasisFromSource","sampleSizeScatStomachTissue","sampleSizeKillsPreyItems","startYear","endYear","startMonth","endMonth","fixedBetweenMonths","startDayOfYear","endDayOfYear","season","minimumElevationInMeters","maximumElevationInMeters","geographicRegion","country","stateProvince","county","municipality","verbatimLocality","protectedAreaHigher","protectedAreaLower","islandGroup","island","studyAreaSize","decimalLatitude","decimalLongitude","georeferenceSources","samplingProtocol","methodQuantification","sourceAuthor","sourceYear","sourceTitle","sourceJournal","sourcePrimaryReference","sourceCollectionReference"
])
print(carnivorous_valid_df.count())