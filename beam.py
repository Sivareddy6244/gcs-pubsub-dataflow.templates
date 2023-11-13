import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.io.gcp.pubsub import WriteToPubSub
import json
import time

class ProcessNewFile(beam.DoFn):
    def process(self, element):
        blob = element
        metadata = {
            'bucket': blob.bucket.name,
            'object': blob.name,
            'size': blob.size,
            'contentType': blob.content_type,
            'generation': blob.generation,
            'metageneration': blob.metageneration,
            'created': time.ctime(blob.time_created.timestamp()),
            'updated': time.ctime(blob.updated.timestamp()),
        }

        # Print the metadata
        print(f"File metadata: {metadata}")

        metadata_str = f"File metadata: {metadata}"

        print(f"New file uploaded: {blob.name}")
        print(metadata_str)

        message = json.dumps({"file_name": blob.name, "metadata": metadata}).encode('utf-8')
        yield PubsubMessage(data=message)

def run():
    # Set your GCS bucket name and Pub/Sub topic name
    bucket_name = 'gcs-pubsub-dataflow'
    topic_name = 'projects/cool-mile-404306/topics/gcs-pubsub-topic'

    # Define the pipeline options
    options = PipelineOptions(
        runner='DataflowRunner',
        project='cool-mile-404306',
        region='us-central1',
        temp_location='gs://gcs-pubsub-dataflow/temp_folder/',
        template_location='gs://gcs-pubsub-dataflow/template_location',
    )

    # Create the pipeline
    p = beam.Pipeline(options=options)

    # Read files from GCS
    files = (
        p
        | "ReadFiles" >> beam.io.ReadFromText(f'gs://{bucket_name}/*')
    )

    # Process new files
    processed_files = (
        files
        | "ProcessNewFile" >> beam.ParDo(ProcessNewFile())
        | "PublishToPubSub" >> WriteToPubSub(topic_name)
    )

    # Run the pipeline
    result = p.run()
    result.wait_until_finish()

if __name__ == '__main__':
    run()
