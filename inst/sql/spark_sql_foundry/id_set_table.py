from transforms.api import transform_df, Output
from pyspark.sql.types import StructType, StructField, LongType


@transform_df(
    Output("@output_path/@name")
)
def function(ctx):
    id_list = @ids
    df = ctx.spark_session.createDataFrame(id_list, LongType())
    df = df.withColumnRenamed("value", "id")
    return df