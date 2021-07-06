# -*- coding: utf-8 -*-
"""
Created on Mon Jul  5 02:58:28 2021

@author: megha
"""

from requests import Session
import os
from os import environ
from time import sleep
import logging
from concurrent import futures
from google.cloud.pubsub_v1 import PublisherClient
from google.cloud.pubsub_v1.publisher.futues import Future




credentials_path = 'C:/Users/megha/Desktop/EGEN/advance-proton-135823-d036ef3632b5.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
class PublishToPubsub:
    def __init__(self):
       

        self.project_id = "meghana_np_project"
        self.topic_id = "crypto_ticker"
        self.publisher_client = PublisherClient()
        self.topic_path = self.publisher_client.topic_path(self.project_id, self.topic_id)
        self.publish_futures = []
        
        
    def get_crypto_ticker_data(self) -> str:
        CRYPTO_CONFIG = {"currency":"USD"}
        url="https://api.nomics.com/v1/currencies/ticker"
        params = {
                "key": environ.get("API_TOKEN",""),

                "convert": CRYPTO_CONFIG["currency"],
                "interval": "id",
                "per-page": "100",
                "page": "1",
                }
        ses = Session()
        res = ses.get(url, params=params, stream=True)
        
        if 200 <= res.status_code < 400:
            logging.info(f"Response - {res.status_code}: {res.text}")
            return res.text
        else:
            raise Exception(f"Failed to fetch API data - {res.status_code}: {res.text}")
            
    def get_callback(self,publish_future: Future, data: str) -> callable:
            def callback(publish_future):
                try:
                    #wait 60 seconds for the publish call to succeed.
                    logging.info(publish_future.result(timeout = 60))
                
                except futures.TimeoutError:
                        logging.error(f"Publishing {data} timed out.")
            return callback
        
    def publish_message_to_topic(self,message: str) -> None:

        publish_future = self.publisher_client.publish(self.topic_path, message.encode("utf-8"))
        
        #Non-blocking. Publish failures are handles in the callback function
        publish_future.add_done_callback(self.get_callback(publish_future,message))
        self.publish_futures.append(publish_future)
        
        #Wait for all the publish futures to resolve before exiting.
        futures.wait(self.publish_futures, return_when=futures.ALL_COMPLETED)
        
        logging.info(f"Published messages with error handler to {self.topic_path}." )
        
if __name__ == "__main__":
    
    init_logging()
    
    svc = PublishToPubsub()
    for i in range(21):
        message = svc.get_crypto_ticker_data()
        svc.publish_message_to_topic(message)
        sleep(90)
        
        
        
