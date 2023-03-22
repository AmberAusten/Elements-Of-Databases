import datetime, logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import WriteToText
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery

    # Implementing the DoFn on format class, splitting the directors column at commas. 
class FormatDirectors(beam.DoFn):
  def process(self, element):
    titleId = element['titleId']
    cast = element['directors']
    nameCount = 0;
    
    split_name = cast.split(',')
    
    if len(split_name) > 1:
        for i in range (0, len(split_name)-1): 
            name = split_name[i] 
            record = {'titleId': titleId, 'directors': name}
            yield record
    else:
        split_name = cast.split(' ')
        name = split_name[0]
        record = {'titleId': titleId, 'directors': name}
    return [record]
           
def run():
     PROJECT_ID = 'still-bank-302722'
     BUCKET = 'gs://the-villagers'
     DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/' #for timestamp and records go straight to the output bucket folder
    
    #runner is dataflow runner instead of direct runner
     options = PipelineOptions(
     flags=None,
     runner='DataflowRunner',
     project=PROJECT_ID,
     job_name='titledirectors', #globally unique
     temp_location=BUCKET + '/temp',
     region='us-central1')

     p = beam.pipeline.Pipeline(options=options)
    
         # selecting the titleId, directors from datamart title crew 
     sql = 'SELECT titleId, directors FROM datamart.Title_crew' #no limit clause, this will process all of out data in a distributive manner
     bq_source = ReadFromBigQuery(query=sql, use_standard_sql=True, gcs_location=BUCKET)

     query_results = p | 'Read from BQ' >> beam.io.Read(bq_source)
        # getting query results and passing them through the ParDo where they get split up. Saved under out_p collection
     out_pcoll = query_results | 'Format Directors' >> beam.ParDo(FormatDirectors())
        #writing to a text file cannot be written on a local file, has to be to our bucket
     out_pcoll | 'Log output' >> WriteToText(DIR_PATH + 'output.txt')
    

     dataset_id = 'datamart'
     table_id = PROJECT_ID + ':' + dataset_id + '.' + 'titleDirectors_Dataflow'
     schema_id = 'titleId:STRING,directors:STRING'
        # Writing output text files contents into Big Query  
     out_pcoll | 'Write to BQ' >> WriteToBigQuery(table=table_id, schema=schema_id, custom_gcs_temp_location=BUCKET)
     
     result = p.run()
     result.wait_until_finish()      


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.ERROR)
  run()
