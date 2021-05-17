import sys
from os.path import exists
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def filter_clients(personal_data_path, financial_data_path, countries_of_interest):
    spark = SparkSession.builder.getOrCreate()
    spark_context = spark.sparkContext
    spark_context.setLogLevel("ERROR")

    log4jLogger = spark_context._jvm.org.apache.log4j
    LOGGER = log4jLogger.LogManager.getLogger(__name__)
    
    LOGGER.info("Transforming personal data")
    personalDF = spark.read.csv(personal_data_path, header=True)
    countries_of_interest = set(countries_of_interest)
    personalDF = personalDF.where(col('country').isin(countries_of_interest))
    personalDF = personalDF.select("id", "email")
    
    LOGGER.info("Transforming financial data")
    financialDF = spark.read.csv(financial_data_path, header=True)
    financialDF = financialDF.select("id", "btc_a", "cc_t")

    LOGGER.info("Join personal and financial data")
    emails_and_details = personalDF.join(financialDF, on='id')
    LOGGER.info("Rename result columns")
    emails_and_details = emails_and_details.\
        withColumnRenamed('id', 'client_identifier').\
        withColumnRenamed('btc_a', 'bitcoin_address').\
        withColumnRenamed('cc_t', 'credit_card_type')
    LOGGER.info("Writing results")
    emails_and_details.write.option("header", True).csv('client_data')


if __name__ == '__main__':
    def is_csv_filename(fname):
        return fname.endswith('.csv') and exists(fname)
    if len(sys.argv) >= 4:
        personal, financial, countries = sys.argv[1], sys.argv[2], sys.argv[3:]
        if is_csv_filename(personal) and is_csv_filename(financial) and 1 <= len(countries) and all(isinstance(item, str) for item in countries):
            filter_clients(personal, financial, countries)
            exit(0)
    print('Sorry, wrong arguments.\nUsage: '
    f'python {__file__} path/to/presonal_data.csv path/to/financial_data.csv country_name_1 country_name_2 .. country_name_N')
        