import datetime, logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import WriteToText
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery

    # Implementing the DoFn on format class, splitting the primary profession column at commas. 
class FormatPrimaryProfession(beam.DoFn):
  def process(self, element):
    p = element['primaryProfession']  
    nconst = element['nconst']
    nameCount = 0;
    
     #If p is not null/none, then split the multiple p's by a comma to place into a record
    if p != None:
        split_profession = p.split(',')
    
        if len(split_profession) > 1:
            for i in range (0, len(split_profession)-1): 
                profession = split_profession[i] 
                record = {'nconst' : nconst, 'primaryProfession': profession}
                yield record
        else:
            split_profession = p.split(' ')
            record = {'nconst' : nconst, 'primaryProfession': split_profession}
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
     job_name='primaryprofessions', #globally unique
     temp_location=BUCKET + '/temp',
     region='us-central1')

     p = beam.pipeline.Pipeline(options=options)
    
         # selecting the nconst, primary profession from datamart name basics 
     sql = 'SELECT primaryProfession, nconst FROM datamart.Name_basics' #no limit clause, this will process all of out data in a distributive manner
     bq_source = ReadFromBigQuery(query=sql, use_standard_sql=True, gcs_location=BUCKET)

     query_results = p | 'Read from BQ' >> beam.io.Read(bq_source)
        # getting query results and passing them through the ParDo where they get split up. Saved under out_p collection
     out_pcoll = query_results | 'Format Primary Profession' >> beam.ParDo(FormatPrimaryProfession())
       #writing to a text file cannot be written on a local file, has to be to our bucket
     out_pcoll | 'Log output' >> WriteToText(DIR_PATH + 'output.txt')
    


     dataset_id = 'datamart'
     table_id = PROJECT_ID + ':' + dataset_id + '.' + 'primaryProfession_Dataflow'
     schema_id = 'nconst:STRING,primaryProfession:STRING'
        # Writing output text files contents into Big Query  
     out_pcoll | 'Write to BQ' >> WriteToBigQuery(table=table_id, schema=schema_id, custom_gcs_temp_location=BUCKET)
        
     
     result = p.run()
     result.wait_until_finish()      


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.ERROR)
  run()
