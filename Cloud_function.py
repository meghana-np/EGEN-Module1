# -*- coding: utf-8 -*-
"""
Created on Mon Jul  5 20:40:51 2021

@author: megha
"""

import logging
from base64 import b64decode
from  pandas import DataFrame
from json import loads
from google.cloud.storage import Client

class LoadToStorage:
    def __init__(self,event,context):
        self.context = context
        self.event = event
        self.bucket_name = "module1"
        
        logging.info(
                f"Function triggered by messageId {self.context.event.id} published at {self.context.timestamp}"
                f"to {self.context.resource['name']}")
        
        if "data" in self.event:
            pubsub_message = b64decode(self.event["data"]).decode('utf-8')
            logging.info(pubsub_message)
            return pubsub_message

        else:
            logging.error("Format Incorrect")
            return ""
        
    def transform_payload_to_dataframe(self, message: str)-> DataFrame:
        try:
            df = DataFrame(loads(message))
            if not df.empty:
                logging.info(f"Created DF with {df.shape[1]}")
        
            
            else:
                logging.warning(f"Created empty df")
            return df
        
    except Exception as e:
        logging.error(f"Error creating data frame {str(e)}")
        raise
        
    def upload_to_bucket(sellf, df: DataFrame, file_name: str = "payload") -> None:
        
        storage_client = Client()
        bucket = storage_client.bucket(self.bucket_name)
        blob = bucket.blob(f"{file_name}.csv")
        blob.upload_from_string(data=df.to_csv(index=False), content_type = "text/csv")
        logging.info(f"File uploaded to {self.bucket_name}")
        
   
    def process(event, context):
    
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    svc = LoadToStorage(event, context)
    message = svc.get_message_data()
    upload_df = svc.transform_payload_to_dataframe(message)
    payload_timestamp = upload_df["price_timestamp"].unique().tolist()[0]
    svc.upload_to_bucket(uplaod_df,"crypto_ticker_data_"+strpayload_timestamp))
    
    