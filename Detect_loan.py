import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam import window
from apache_beam.transforms.trigger import AfterWatermark, AfterProcessingTime, AccumulationMode, AfterCount, Repeatedly
import os
from datetime import datetime
import argparse
import json
import logging
import time
from typing import Any, Dict, List
from apache_beam.transforms.window import FixedWindows

# คำนวนคะแนน 
def calculate_points(element):

  customer_id,realtionship_id,card_type, max_limit, spent,cash_withdrawn,payment_cleared,payment_date = element
  # example [CT55555,R_5555,Issuers,500,490,38,101,30-01-2018]
  
  spent = int(spent)    # spent = 490
  payment_cleared = int(payment_cleared)   #payment_cleared = 101
  max_limit = int(max_limit)               # max_limit = 500
  
  defaulter_points = 0
  
  # หากการชำระเงินน้อยกว่า 80% ของยอดจ่ายจริง ให้รับคะแนน 1 คะแนน
  if payment_cleared < (spent * 0.8): 
     defaulter_points += 1                                                # defaulter_points =  1 
 
  # หากยอดการใช้จ่ายเท่ากับ 100% ของวงเงินสูงสุดและยังมีการชำระเงินที่ค้างอยู่ใด ๆ ให้รับคะแนน 1 คะแนน
  if (spent == max_limit) and (payment_cleared < spent): 
     defaulter_points += 1                                                # defaulter_points =  2
   
  if (spent == max_limit) and (payment_cleared < (spent*0.8)): 
     defaulter_points += 1                                                # defaulter_points = 3
                                  
  return customer_id, defaulter_points                                     # {CT55555,3}

# format output ของ personal defaulter
def format_output(sum_pair):
  key_name, miss_months = sum_pair
  return str(key_name) + ', ' + str(miss_months) + ' missed'

# format output ของ personal defaulter
def format_result(sum_pair):
  key_name, points = sum_pair
  return str(key_name) + ', ' + str(points) + 'fraud_points'



# สกัดเดือนและบันทึกลงใน list
def calculate_month(input_list):        
  # input  [CT55555,Serviceman,LN_5555,Medical Loan,26-01-2018, 2000, 30-01-2018]                                     
  # แปลงคอลัมน์ payment_date เป็นวันที่และสกัดเดือนของการชำระเงิน 
  payment_date = datetime.strptime(input_list[4].rstrip().lstrip(), '%d-%m-%Y')  # payment_date = 30-01-2018
  input_list.append(str(payment_date.month))                                     # [CT55555,Serviceman,LN_5555,Medical Loan,26-01-2018, 2000, 30-01-2018,01]     
  return input_list 

def calculate_personal_loan_defaulter(input):       
    #key -> CT55555   value --> [01,05,06,07,08,09,10,11,12]
    max_allowed_missed_months = 2
    max_allowed_consecutive_missing = 1
    customer_id, months_list = input                                   
    # [CT55555,Serviceman,LN_5555,Personal Loan,25-01-2018,50000,25-01-2018]
    months_list.sort()
    sorted_months = months_list                                 # sorted_months = [01,05,06,07,08,09,10,11,12]
    total_payments = len(sorted_months)                         # total_payments = 10
    
    missed_payments = 12 - total_payments                       # missed_payments = 2

    if missed_payments > max_allowed_missed_months:             # false
       return customer_id, missed_payments                      #  N/A
    
    consecutive_missed_months = 0

    temp = sorted_months[0] - 1                                 # temp = 0
    if temp > consecutive_missed_months:                        # false
        consecutive_missed_months = temp                        #NA

    temp = 12 - sorted_months[total_payments-1]                  
    if temp > consecutive_missed_months:
        consecutive_missed_months = temp                        # temp = 0

    for i in range(1, len(sorted_months)):                      # [01,05,06,07,08,09,10,11,12]
        temp = sorted_months[i] - sorted_months[i-1] -1         # temp = 5-1-1 = 3
        if temp > consecutive_missed_months:
            consecutive_missed_months = temp                    # consecutive_missed_months = 3
    
    if consecutive_missed_months > max_allowed_consecutive_missing:
       return customer_id, consecutive_missed_months                   # CT68554, 3
    
    return customer_id, 0 

# return key และ ค่าของ key นั้นๆ
def return_tuple(element):
  thisTuple=element.split(',')
  return (thisTuple[0],thisTuple[1:])    

# convert dictionary to list
def convert_json(element_list):
    json_data = json.loads(element_list)
    element_list = list(json_data.values())
    return element_list

# ฟังก์ชันสำหรับรับ input 
def run(
    input_subscription_card: str,
    input_subscription_loan: str,
    output_table: str,
    window_interval_sec: int = 60,
    beam_args: List[str] = None,
    
) -> None:
    """Build and run the pipeline."""
    options = PipelineOptions(beam_args, save_main_session=True, streaming=True)
    with beam.Pipeline(options=options) as pipeline:
        card_defaulter = (
                        pipeline
                        | 'Read from pub sub Card' >> beam.io.ReadFromPubSub(subscription= input_subscription_card)
                        | 'Apply fixed window Card' >> beam.WindowInto(FixedWindows(window_interval_sec))
                        | 'Parse data Card' >> beam.Map(convert_json)
                        | 'Calculate defaulter points' >> beam.Map(calculate_points)                            
                        | 'Combine points for defaulters' >> beam.CombinePerKey(sum)                            # key--> CT55555,value --> 6 
                        | 'Filter card defaulters' >> beam.Filter(lambda element: element[1] > 0)
                        | 'Format output' >> beam.Map(format_result)                                            # CT55555,6 fraud_points
                        | 'tuple ' >> beam.Map(return_tuple)  
                        )		    

        personal_loan_defaulter = (
                                    pipeline
                                    | 'Read from pub sub Loan' >> beam.io.ReadFromPubSub(subscription= input_subscription_loan)
                                    | 'Apply fixed window Loan' >> beam.WindowInto(FixedWindows(window_interval_sec))
                                    | 'Parse data Loan' >> beam.Map(convert_json)
                                    | 'Filter personal loan' >> beam.Filter(lambda element : (element[3]).rstrip().lstrip() == 'Personal_Loan')
                                    | 'Split and Append New Month Column' >> beam.Map(calculate_month) # [CT55555,Serviceman,LN_5555,Medical Loan,26-01-2018, 2000, 30-01-2018, 01]  
                                    | 'Make key value pairs loan' >> beam.Map(lambda elements: (elements[0],int(elements[7])) ) 
                                    | 'Group personal loan based on month' >> beam.GroupByKey()                                  # CT55555,[01,05,06,07,08,09,10,11,12]
                                    | 'Check for personal loan defaulter' >> beam.Map(calculate_personal_loan_defaulter)          # CT55555,3
                                    | 'Filter only personal loan defaulters' >> beam.Filter(lambda element: element[1] > 0)
                                    | 'Format personal loan output' >> beam.Map(format_output)  # CT55555,6 missed
                                    | 'tuple1 ' >> beam.Map(return_tuple)  
                                )   
                                
                                
        both_defaulters = (         # สกัดข้อมูล และบันทึกลงใน Bigquery
                                    {'card_defaulter': card_defaulter, 'personal_defaulter': personal_loan_defaulter}
                                    | beam.CoGroupByKey() 
                                    | 'Extract Combined Information' >> beam.Map(lambda elem: {
                                        'Customer_id': elem[0],
                                        'Card_Defaulter_Fraud_points': ','.join(str(value) for value in elem[1]['card_defaulter'][0]) if elem[1]['card_defaulter'] else '',
                                        'Personal_Loan_Defaulter_Consecutive_Missing': ','.join(str(value) for value in elem[1]['personal_defaulter'][0]) if elem[1]['personal_defaulter'] else '',
                                        'Both_Defaulters': 'Yes' if elem[1]['card_defaulter'] and elem[1]['personal_defaulter'] else 'No'
                                    })
                                    | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
                                        output_table,
                                        schema='Customer_id:STRING,Card_Defaulter_Fraud_points:STRING,Personal_Loan_Defaulter_Consecutive_Missing:STRING,Both_Defaulters:STRING',
                                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                    )
)
           

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output_table",
        help="Output BigQuery table for results specified as: "
        "PROJECT:DATASET.TABLE or DATASET.TABLE.",
    )
    parser.add_argument(
        "--input_subscription_card",
        help="Input PubSub subscription for card_defaulter.",
    )
    parser.add_argument(
        "--input_subscription_loan",
        help="Input PubSub subscription for personal_loan_defaulter.",
    )
    parser.add_argument(
        "--window_interval_sec",
        default=60,
        type=int,
        help="Window interval in seconds for grouping incoming messages.",
    )
    args, beam_args = parser.parse_known_args()

    run(
        input_subscription_card=args.input_subscription_card,
        input_subscription_loan=args.input_subscription_loan,
        output_table=args.output_table,
        window_interval_sec=args.window_interval_sec,
        beam_args=beam_args,
    )