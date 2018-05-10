from email_sender import EmailSender
import sys
import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import csv
import json
#built-in aditya support
def csvToHtml(fileName):
	return pd.read_csv(fileName).to_html(index = False);
html_string = csvToHtml('C:\QlikDataFiles\gear_kpi\Gear KPI.csv')
EmailSender().send(html_string)
