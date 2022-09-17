from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

def write_parquet(dataframe, path):
    dataframe.coalesce(1).write.format('parquet').mode('append').save(path)


class ELT():

    def read_raw(self):
        print("Extract")
        self.df_s = spark.read.parquet('data.parquet')

    def remove_duplicates(self):
        self.dropDisDF_inter = self.df_s.dropDuplicates(["transaction_timestamp", "from_address", "to_address"])

    def add_date(self):
        self.dropDisDF = self.dropDisDF_inter.withColumn("date", to_date("transaction_timestamp"))

    def top5_h_amount(self):
        print("Daily top-5 NFT tokens with the highest amount")
        window = Window.partitionBy(self.dropDisDF['date']).orderBy(self.dropDisDF['amount'].desc())
        self.top5_h_amount_df  = self.dropDisDF.select('token_id', 'date', row_number().over(window).alias('daily_position')).filter(
            col('daily_position') <= 5)

    def top5_am_amount(self):
        print('Daily top-5 NFT tokens with the accumulated transaction amounts')
        self.accumDF = self.dropDisDF.groupBy(['token_id', 'date']).sum("amount").withColumnRenamed('sum(amount)',
                                                                                          'accumulated_amount')
        window_1 = Window.partitionBy(self.accumDF['date']).orderBy(self.accumDF['accumulated_amount'].desc())
        self.top5_am_amount_df =self.accumDF.select('*', row_number().over(window_1).alias('daily_position')).filter(col('daily_position') <= 5)

    def get_dta(self):
        print('Parquet with date, token_id, amount')
        df_dta = self.dropDisDF.select('token_id', 'date', 'amount')
        write_parquet(df_dta, 'df_dta')

    def get_dtaa(self):
        print('Parquet with date, token_id, accumulated amount')
        df_dtaa = self.accumDF.select('token_id','date','accumulated_amount')
        write_parquet(df_dtaa,'df_dtaa')

    def process(self):
        self.read_raw()
        self.remove_duplicates()
        self.add_date()
        self.top5_h_amount()
        self.top5_h_amount_df.show()
        self.top5_am_amount()
        self.top5_am_amount_df.show()
        self.get_dta()
        self.get_dtaa()


if __name__ == '__main__':
    spark = SparkSession.builder.appName('Pratcise2').getOrCreate()
    etl = ELT()
    etl.process()
    spark.stop()