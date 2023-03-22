import logging
import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery


    # Implementing the DoFn on format class, splitting the known for titles column at commas. 
class FormatKnownForTitles(beam.DoFn):
  def process(self, element):
    kft = element['knownForTitles']
    nconst = element['nconst']
    nameCount = 0;
    
    split_titles = kft.split(',')

    
    if len(split_titles) > 1:
        for i in range (0, len(split_titles)-1): 
            titles = split_titles[i] 
            record = {'nconst': nconst,'knownForTitles': titles}
            yield record
    else:
        split_titles = kft.split(' ')
        titles = split_titles[0]
        record = {'nconst': nconst,'knownForTitles': titles}
    return [record]

           
def run():
     PROJECT_ID = 'still-bank-302722'
     BUCKET = 'gs://the-villagers/temp'

     options = {
     'project': PROJECT_ID
     }
     opts = beam.pipeline.PipelineOptions(flags=[], **options)

     p = beam.Pipeline('DirectRunner', options=opts)
         # selecting the nconst, known for titles from datamart name basics and limiting by 490 
     sql = 'SELECT knownForTitles, nconst FROM datamart.Name_basics limit 490'
     bq_source = ReadFromBigQuery(query=sql, use_standard_sql=True, gcs_location=BUCKET)

     query_results = p | 'Read from BQ' >> beam.io.Read(bq_source)
        # getting query results and passing them through the ParDo where they get split up. Saved under out_p collection
     out_pcoll = query_results | 'Format Known For Titles' >> beam.ParDo(FormatKnownForTitles())
        # writing to local output text file 
     out_pcoll | 'Log output' >> WriteToText('output.txt')
    

     dataset_id = 'datamart'
     table_id = PROJECT_ID + ':' + dataset_id + '.' + 'knownForTitles_Beam'
     schema_id = 'knownForTitles:STRING,nconst:STRING'
        # Writing output text files contents into Big Query  
     out_pcoll | 'Write to BQ' >> WriteToBigQuery(table=table_id, schema=schema_id, custom_gcs_temp_location=BUCKET)

     
     result = p.run()
     result.wait_until_finish()      


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.ERROR)
  run()
