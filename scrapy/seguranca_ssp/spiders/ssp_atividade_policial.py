# -*- coding: utf-8 -*-
import scrapy
import json
import pandas as pd
import time
import numpy as np
import csv
import os


def append_to_csv(df, file,path):
    file_pre=file+"_MODELO.csv"
    file = path + file  +".csv"
    
    if not os.path.exists(file):

        df_modelo = pd.read_csv(file_pre,encoding='utf-8')

        df_modelo.to_csv(file, index = False, header = True, encoding='utf-8')

        with open(file) as f:
            header = next(csv.reader(f))
        columns = df.columns
        for column in set(header) - set(columns):
            df[column] = ''
        df = df[header]
        df.to_csv(file, index = False, header = False, mode = 'a', encoding='utf-8')
        
    else:
        with open(file) as f:
            header = next(csv.reader(f))
        columns = df.columns
        for column in set(header) - set(columns):
            df[column] = ''
        df = df[header]
        df.to_csv(file, index = False, header = False, mode = 'a', encoding='utf-8')

class IcmsSpider(scrapy.Spider):
    name = 'icms'
    site_url = 'http://www.ssp.sp.gov.br/Estatistica/Pesquisa.aspx'
    # allowed_domains = ['www.fazenda.sp.gov.br/RepasseConsulta/Consulta/repasse.aspx']
    start_urls = [site_url]

    def parse(self, response):
        #list of all cities codes
        years = [str(i) for i in range(2002,2020)]
        regiao = '0'
        municipipos =[str(i) for i in range(1,646)]
        delegacia = '0'
        #iterate over all cities and years

        for municipipo in municipipos:
            for year in years:
                EVENTVALIDATION = response.xpath("//*[@id='__EVENTVALIDATION']/@value").extract_first()
                VIEWSTATE = response.xpath("//*[@id='__VIEWSTATE']/@value").extract_first()
                VIEWSTATEGENERATOR = response.xpath("//*[@id='__VIEWSTATEGENERATOR']/@value").extract_first()
                # VIEWSTATEGENERATOR = '79344604'
                data = {'__EVENTTARGET':'ctl00$conteudo$btnPolicial',
                        '__VIEWSTATE': VIEWSTATE,
                        '__VIEWSTATEGENERATOR': VIEWSTATEGENERATOR,
                        '__EVENTVALIDATION': EVENTVALIDATION,
                        "ctl00$conteudo$ddlAnos":year,
                        'ctl00$conteudo$ddlRegioes':regiao,
                        'ctl00$conteudo$ddlMunicipios':municipipo,
                        'ctl00$conteudo$ddlDelegacias':delegacia
                    }
                header = {
                    'Content-Type':' application/x-www-form-urlencoded'
                }
                yield scrapy.FormRequest(self.site_url, headers=header, formdata = data, callback=self.parse_months,  dont_filter=False)

    def parse_months(self,response):
        # print(response.text)
        # #get the columns

        cols = response.xpath('//*[@id="conteudo_repAnos_divGrid_0"]//th/font/text()').extract()
        # print('======HERE======','\n',cols)
        # print(cols)

        rows = response.xpath('//*[@id="conteudo_repAnos_divGrid_0"]//tr')
        df = pd.DataFrame(columns=cols)
        for row in rows:
            row_to_add = row.xpath('.//td/text()').extract()
            # print(list(row_to_add))
            if row_to_add == []:
                pass
            else:
                try:
                    df.loc[len(df), :] = row_to_add
                except:
                    df.loc[len(df), :] = [np.nan for i in range(len(cols))]


        cols_name = ['mes']+df['Natureza'].tolist()
        df = df.T.reset_index()
        df.columns = cols_name
        mask = (df['mes']!='Natureza') & (df['mes']!='Total')
        df = df[mask]
        


        selected = response.xpath('//*[@selected="selected"]/text()').extract()
        # print(selected)
        print('\n\n',selected[2] ,selected[0],'\n\n')


        df['ano'] = selected[0]
        df['regiao'] = selected[1]
        df['municipio'] = selected[2]
        df['delegacia'] = selected[3]
        df['data_de_captura'] = pd.Timestamp.now()
        # print(df)

        final_cols = ['municipio','regiao','delegacia','ano'] + cols_name + ['data_de_captura']
        df = df[final_cols]

        path = '../../../dados/'
        append_to_csv(df,'ssp_atividade_policial',path)

        # time.sleep(10)