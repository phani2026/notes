from pipelineblocksdk.construct.base.StreamBlock import StreamBlock
from pipelineblocksdk.util.async_util import async
from pipelineblocksdk.api.Singleton import Singleton
from pipelineblocksdk.construct.base.BatchBlock import BatchBlock
from pipelineblocksdk.data.spark.SparkConfCustom import SparkConfCustom


# Following code is the definition for a batch block

class MyBlock(BatchBlock, Singleton):

    # This is the entry point for the block. This is a mandatory function.
    def run(self):
        print('Run function says: Hello, world!')
        print("Inputs available:", self.input_dict)
        spark = SparkConfCustom().get_spark_session()
        print(spark.sparkContext.getConf().getAll())
        path1 = '/bigbrain/feat.csv'
        path2 = '/bigbrain/feat.csv'
        left_df = spark.read.format("csv").option("header", "true").option("delimiter", ",").load(path1)
        right_df = spark.read.format("csv").option("header", "true").option("delimiter", ",").load(path2)
        left_df.createOrReplaceTempView("left")
        right_df.createOrReplaceTempView("right")



        df2 = spark.sql("SELECT * from left l inner join right r ON l.CUST_ID == r.CUST_ID")

        new_column_name_list = df2.columns
        for col in new_column_name_list:
            count = new_column_name_list.count(col)
            if count > 1:
                idx = new_column_name_list.index(col)
                new_column_name_list[idx] = col+'_1'

        df2 = df2.toDF(*new_column_name_list)

        df2.write.mode('overwrite').csv('/bigbrain/test-spark.csv')

        # Set the output parameter
        output_dict = dict()
        output_dict["queueTopicName"] = ''

        # Set the status of the block as completed
        self.block_status = "COMPLETED"

        return output_dict
