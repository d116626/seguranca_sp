# -*- coding: utf-8 -*-
import scrapy
import json
import pandas as pd
import time


class IcmsSpider(scrapy.Spider):
    name = 'icms'
    site_url = 'http://www.ssp.sp.gov.br/Estatistica/Pesquisa.aspx'
    # allowed_domains = ['www.fazenda.sp.gov.br/RepasseConsulta/Consulta/repasse.aspx']
    start_urls = [site_url]

    def parse(self, response):
        #list of all cities codes
        year = '2019'
        regiao = '10'
        municipipo ='1' 
        delegacia = '0'
        #iterate over all cities and years


        EVENTVALIDATION = response.xpath("//*[@id='__EVENTVALIDATION']/@value").extract_first()
        VIEWSTATE = response.xpath("//*[@id='__VIEWSTATE']/@value").extract_first()
        VIEWSTATEGENERATOR = response.xpath("//*[@id='__VIEWSTATEGENERATOR']/@value").extract_first()
        # VIEWSTATEGENERATOR = '79344604'
        data = {'__EVENTTARGET':' ctl00$conteudo$ddlAnos',
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
        print(response.text)
        
        
        # #get the city code
        # city_codes = response.xpath('//*[@selected="selected"]/@value').extract()
        # city_code = city_codes[0]

        # #get the city name and the year
        # city_year = response.xpath('//*[@selected="selected"]/text()').extract()
        # city_name=city_year[0]
        # year     =city_year[1]
        # print('\n\n',city_code,city_name ,year,'\n\n')
        
        # #get the columns
        # cols = response.xpath('//*[@id="ConteudoPagina_tbRepasse"]/tr//table//th/b/text()').extract()
        # # print(cols)
        
        # #get the months
        # months  = response.xpath('//*[@id="ConteudoPagina_tbRepasse"]/tr//table/tr/td/font//font/text()').extract()      
        # months.remove('Total')
        # # print(months)

        # #get the values
        # values = response.xpath('//*[@bgcolor="#EEEEEE"]/text()').extract()
        # # print(values)

        # #create the columns with values
        # icms=[]
        # ipva=[]
        # fund_exp=[]
        # comp=[]
        # total=[]
        # for j in range(len(cols)):
        #     for i in range(j,len(values),5):
        #         if j==0:
        #             icms.append(values[i])
        #         if j==1:
        #             ipva.append(values[i])
        #         if j==2:
        #             fund_exp.append(values[i])
        #         if j==3:
        #             comp.append((values[i]))
        #         if j==4:
        #             total.append((values[i]))
        
        # #criate the dict to make a datafame
        # final_table = {}
        # final_table['city_code'] = [city_code for i in months]
        # final_table['city_name'] = [city_name for i in months]
        # final_table['year'] = [year for i in months]
        # final_table[cols[0]] = months
        # final_table[cols[1]] = icms
        # final_table[cols[2]] = ipva
        # final_table[cols[3]] = fund_exp
        # final_table[cols[4]] = comp
        # final_table[cols[5]] = total

        # #create the dataframe to save 
        # df = pd.DataFrame.from_dict(final_table)
        # #create a column with date_time of capture
        # df['data_de_captura'] = pd.Timestamp.now()
        # df.to_csv('icms_2.csv',mode='a', header=False ,index=False, encoding='utf-8')