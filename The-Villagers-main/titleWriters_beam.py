import logging
import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery

    # Implementing the DoFn on format class, splitting the writers column at commas. 

class FormatWriters(beam.DoFn):
  def process(self, element):
    titleId = element['titleId']
    cast = element['writers']
    nameCount = 0;
    
    split_name = cast.split(',')
    
    if len(split_name) > 1:
        for i in range (0, len(split_name)-1): 
            name = split_name[i] 
            record = {'titleId': titleId, 'writers': name}
            yield record
    else:
        split_name = cast.split(' ')
        name = split_name[0]
        record = {'titleId': titleId, 'writers': name}
    return [record]
           
def run():
     PROJECT_ID = 'still-bank-302722'
     BUCKET = 'gs://the-villagers/temp'

     options = {
     'project': PROJECT_ID
     }
     opts = beam.pipeline.PipelineOptions(flags=[], **options)

     p = beam.Pipeline('DirectRunner', options=opts)
         # selecting the titleId, writers from datamart title crew and limiting by 490 
     sql = 'SELECT titleId, writers FROM datamart.Title_crew limit 490'
     bq_source = ReadFromBigQuery(query=sql, use_standard_sql=True, gcs_location=BUCKET)

     query_results = p | 'Read from BQ' >> beam.io.Read(bq_source)
        # getting query results and passing them through the ParDo where they get split up. Saved under out_p collection
     out_pcoll = query_results | 'Format Writers' >> beam.ParDo(FormatWriters())
        # writing to local output text file 
     out_pcoll | 'Log output' >> WriteToText('output.txt')
    


     dataset_id = 'datamart'
     table_id = PROJECT_ID + ':' + dataset_id + '.' + 'titleWriters_Beam'
     schema_id = 'titleId:STRING,writers:STRING'
        # Writing output text files contents into Big Query  
     out_pcoll | 'Write to BQ' >> WriteToBigQuery(table=table_id, schema=schema_id, custom_gcs_temp_location=BUCKET)
     
     result = p.run()
     result.wait_until_finish()      


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.ERROR)
  run()
