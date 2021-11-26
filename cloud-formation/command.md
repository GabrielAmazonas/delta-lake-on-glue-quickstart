aws cloudformation deploy --profile dev --template-file GlueJobPySparkDelta.yaml --stack-name glue-job-delta-resource --capabilities CAPABILITY_NAMED_IAM --parameter-overrides S3BucketName=glue-delta-bucket DeltaSparkJarName=delta-core_2.12-1.0.0.jar GlueJobName=glue-job-delta-resource DeltaJobName=job.py

