# -*- coding: utf-8 -*-
"""
Created on Wed Apr  5 10:41:01 2023

@author: C050819
"""
# %%
import re
from infra_copel.ftp.base import FTPBase


ftp = FTPBase('refinitiv_ftp')
ftp.pwd()
ftp.cd('/PCO_Copel_Geracao/PCO_Copel_Geracao/Live/S-Am_Power/Hydrology/')
ftp.pwd()
ftp.ls()
df = _
ftp.connect()

list_files = ftp.ls()
filename = list_files.iloc[2]['filename']

ftp.read_csv(filename, sep='|', header=1)

ftp.ftp.voidcmd('NOOP')

