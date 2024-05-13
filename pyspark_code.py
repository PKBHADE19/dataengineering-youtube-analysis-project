import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
#AmazonS3_node1715262188516 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://de-youtube-dev-raw/you-tube/raw_statastics/"], "recurse": True}, transformation_ctx="AmazonS3_node1715262188516")

predicate_pushdown = "region in ('ca','gb','us','de','fr','in','jp')"

datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "de_youtube_raw", table_name = "raw_statastics", transformation_ctx = "datasource0", push_down_predicate = predicate_pushdown)



# Script generated for node Change Schema
ChangeSchema_node1715262143077 = ApplyMapping.apply(frame=datasource0, mappings=[("video_id", "string", "video_id", "string"), ("trending_date", "string", "trending_date", "string"), ("title", "string", "title", "string"), ("channel_title", "string", "channel_title", "string"), ("category_id", "long", "category_id", "long"), ("publish_time", "string", "publish_time", "string"), ("tags", "string", "tags", "string"), ("views", "long", "views", "long"), ("likes", "long", "likes", "long"), ("dislikes", "long", "dislikes", "long"), ("comment_count", "long", "comment_count", "long"), ("thumbnail_link", "string", "thumbnail_link", "string"), ("comments_disabled", "string", "comments_disabled", "boolean"), ("ratings_disabled", "string", "ratings_disabled", "boolean"), ("video_error_or_removed", "string", "video_error_or_removed", "boolean"), ("description", "string", "description", "string"),("region", "string", "region", "string")], transformation_ctx="ChangeSchema_node1715262143077")

resolvechoice2 = ResolveChoice.apply(frame = ChangeSchema_node1715262143077, choice = "make_struct", transformation_ctx = "resolvechoice2")
dropnullfields3 = DropNullFields.apply(frame = resolvechoice2, transformation_ctx = "dropnullfields3")

datasink1 = dropnullfields3.toDF().coalesce(1)
df_final_output = DynamicFrame.fromDF(datasink1, glueContext, "df_final_output")

# Script generated for node Amazon S3
AmazonS3_node1715262296221 = glueContext.write_dynamic_frame.from_options(frame=df_final_output, connection_type="s3", format="glueparquet", connection_options={"path": "s3://de-youtube-dev-cleaned/you-tube/raw-statstics/", "partitionKeys": ["region"]}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1715262296221")

job.commit()