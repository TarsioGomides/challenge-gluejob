import sys
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job


logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_format_and_options(input_path):
    if input_path.endswith('.json'):
        return 'json', {"multiline": True}
    elif input_path.endswith('.csv'):
        return 'csv', {}
    else:
        raise ValueError(f"Unsupported file format for path: {input_path}")

# Pega os argumentos passados pela Lambda/Glue Job
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'input_path', 'output_path'])
logger.info(f'args: {args}')

if not args['input_path'] or not args['output_path']:
    logger.error("input_path e output_path should be provided")
    sys.exit(1)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

try:
    input_path = args['input_path']
    format_type, format_options = get_format_and_options(input_path)
    logger.info(f'format_type: {format_type}')
    logger.info(f'format_options: {format_options}')

    data_frame = glueContext.create_dynamic_frame.from_options(
        format_options=format_options,
        connection_type="s3",
        format=format_type,
        connection_options={"paths": [input_path]},
    )
except Exception as e:
    logger.error(f'Error to load data frame {str(e)}')
    sys.exit(1)

output_path = args['output_path']
try:
    glueContext.write_dynamic_frame.from_options(
        frame=data_frame,
        connection_type="s3",
        format="parquet",
        connection_options={"path": output_path},
    )
    logger.info(f'data written successfully: {output_path}')
except Exception as e:
    logger.error(f'Error to write data: {str(e)}')
    sys.exit(1)

job.commit()
