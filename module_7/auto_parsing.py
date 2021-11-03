# -*- coding: utf-8 -*-
"""
Created on Fri Sep 17 19:32:26 2021

@author: Nata

Parsing auto sites according columns of test.csv for Kaggle competition

https://www.kaggle.com/c/sf-dst-car-price-prediction/data?select=test.csv
1. auto.ru

"""

import util_pg
import psycopg2
import re
import time
import datetime

import requests 
from bs4 import BeautifulSoup   
from requests.exceptions import HTTPError
from fake_useragent import UserAgent    #для маскировки.

ua = UserAgent(cache=True).chrome

dict_super_gen = {'Объем двигателя, см³': 'displacement',
 'Тип двигателя': 'engine_type',
 'Привод': 'gear_type',
 'Коробка': 'transmission',
 'Мощность': 'power',
 'Максимальная мощность':'power_kvt',
 'Клиренс': 'clearance_min',
 'Разгон': 'acceleration',
 'Расход': 'fuel_rate'
 }


dict_ru_en = {'Объем двигателя, см³': 'displacement',
              'Объем': 'displacement',
  'Топливо': 'fuel',  
  'Марка топлива': 'Fuel grade',            
  'Страна марки': 'vendor',   
  'Класс автомобиля': 'class',   
  'Количество дверей': 'numberOfDoors', 
  'Количество мест': 'Seats',            
 'Тип двигателя': 'engine_type',
 'Привод': 'gear_type',
'Тип привода': 'gear_type',            
 'Коробка': 'transmission',
 'Коробка передач': 'transmission',     
 'Количество передач': 'Number of gears',             
 'Мощность': 'power',
 'Максимальная мощность, л.с./кВт при об/мин':'power_kvt',             
 'Максимальная мощность':'power_kvt',
 'Клиренс': 'clearance_min',
 'Разгон': 'acceleration',
 'Разгон до 100 км/ч, с': 'acceleration',              
 'Расход': 'fuel_rate',
 'Расход топлива, л город/трасса/смешанный': 'fuel_rate',             
 'Подушка безопасности водителя': 'airbag-driver',
 'Электростеклоподъёмники передние': 'electro-window-front',
 'Регулировка руля по вылету': 'wheel-configuration1',
 'Антиблокировочная система': 'abs',
 'Антиблокировочная система (ABS)': 'abs',
 'Иммобилайзер': 'immo',
 'Центральный замок': 'lock',
 'Подушка безопасности пассажира': 'airbag-passenger',
 'Электрообогрев боковых зеркал': 'mirrors-heat',
 'Электропривод зеркал': 'electro-mirrors',
 'Бортовой компьютер': 'computer',
 'Аудиоподготовка': 'audiopreparation',
 'Регулировка руля по высоте': 'wheel-configuration2',
 'Электростеклоподъёмники задние': 'electro-window-back',
 'Подушки безопасности боковые': 'airbag-side',
 'Система стабилизации': 'esp',
 'Система стабилизации (ESP)': 'esp',
 'Подогрев передних сидений': 'front-seats-heat',
 'Аудиосистема': 'audiosystem-cd',
 'Складывающееся заднее сиденье': 'seat-transformation',
 'ptf': 'ptf',
 'Климат-контроль 1-зонный': 'climate-control-1',
 'Крепление детского кресла': 'isofix',
 'Кондиционер': 'condition',
 'Отделка кожей рулевого колеса': 'wheel-leather',
 'Мультифункциональное рулевое колесо': 'multi-wheel',
 'Подушки безопасности оконные (шторки)': 'airbag-curtain',
 'Круиз-контроль': 'cruise-control',
 'AUX':'aux',
 'Третий задний подголовник': 'third-rear-headrest',
 'Консоль': 'front-centre-armrest',
 'Датчик дождя': 'rain-sensor',
 'Усилитель руля': 'wheel-power',
 'Датчик света': 'light-sensor',
 'Розетка 12V': '12v-socket',
 'USB': 'usb',
 'hcc': 'hcc',
 'Bluetooth': 'bluetooth',
 'Парктроник задний': 'park-assist-r',
 'servo': 'servo',
 'Омыватель фар': 'light-cleaner',
 'Кожа (Материал салона)': 'leather',
 'Сигнализация': 'alarm',
 'Электроскладывание зеркал': 'auto-mirrors',
 'Автоматический корректор фар': 'automatic-lighting-control',
 'Датчик давления в шинах': 'tyre-pressure',
 'Металлик': 'paint-metallic',
 'Запуск двигателя с кнопки': 'start-button',
 'Прикуриватель и пепельница': 'ashtray-and-cigarette-lighter',
 'Панорамная крыша': 'panorama-roof',
 'Камера задняя': 'rear-camera',
 'Декоративные молдинги': 'body-mouldings',
 'Система доступа без ключа': 'keyless-entry',
 'Рейлинги на крыше': 'roof-rails',
 'Подогрев руля': 'wheel-heat',
 'Блокировка замков задних дверей': 'power-child-locks-rear-doors',
 'Память сидений': 'seat-memory',
 'Диски 17': '17-inch-wheels',
 'Датчик звука': 'volume-sensor',
 'Диски 18': '18-inch-wheels',
 'Охлаждаемый перчаточный ящик': 'cooling-box',
 'Регулируемый педальный узел': 'steering-wheel-gear-shift-paddles',
 'Подогрев задних сидений': 'rear-seats-heat',
 'Люк': 'hatch',
 'Система управления дальним светом': 'adaptive-light',
 'Система предотвращения столкновения': 'collision-prevention-assist',
 'Вентиляция передних сидений': 'front-seats-heat-vent',
 'Камера 360': '360-camera',
 'Подушки безопасности боковые': 'airbag-rear-side',
 'Диски 19': '19-inch-wheels',
  'Функция складывания спинки сиденья пассажира': 'passenger-seat-updown',
 'Система контроля слепых зон': 'blind-spot',
 'Третий ряд сидений': 'third-row-seats',
 'Голосовое управление': 'voice-recognition',
  'Диски 20': '20-inch-wheels',
  'Система контроля слепых зон': 'roller-blinds-for-rear-side-windows',
 'Система контроля за полосой движения': 'traffic-sign-recognition',
 'Память передних сидений': 'driver-seat-memory',
  'Активная подвеска': 'activ-suspension',
 'Вентиляция задних сидений': 'rear-seat-heat-vent',
 'Ламинированные боковые стекла': 'laminated-safety-glass',
 'Передняя камера': 'front-camera',
 'Длина': 'Length',
 'Ширина': 'Width',
 'Высота': 'High',
 'Колёсная база': 'Wheelbase',
 'Ширина передней колеи': 'Front track width',
 'Ширина задней колеи': 'Rear track width',
  'Размер колёс': 'Wheel size',
  'Объем багажника мин/макс, л': 'Trunk volume min/max, l',
  'Объём топливного бака, л': 'Fuel tank capacity, l',
  'Снаряженная масса, кг': 'Curb weight, kg',
   'Полная масса, кг': 'Gross weight, kg',
  'Тип передней подвески': 'Type of front suspension',
  'Тип задней подвески': 'Rear suspension type',
  'Передние тормоза': 'Front brakes',
'Задние тормоза': 'Rear brakes',
'Максимальная скорость, км/ч': 'Maximum speed, km/h',
'Расположение двигателя': 'Engine location',
 'Тип наддува': 'Boost type',
 'Максимальный крутящий момент, Н*м при об/мин': 'Maximum torque, N * m at rpm'             
             }



def cursor_execute(cursor, query, description):
    
    '''
    Parameters
    ----------
    cursor - cursor
    **kwargs :
        description - str
        query - text of sql request  

    Returns : None
    -------
    '''

    try:
        cursor.execute(query)
        
    except Exception as e:
        print("%s :"%(description),e)
        
    return     

def insert_db(conn, table, data_array, dictColumns):
    '''
    
    Parameters
    ----------
    conn : connection with table of db
    table : table name to insert data
    data_array : array 2D with data
    dictColumns : dictionary of table columns to write data
    Returns
    -------
    None.

    '''
    #create cursor to insert data
    cur = conn.cursor() 
    
    zag, vals, evals = '', '', ''
     
    #select * from json_array_elements('[1,true, [2,false]]')
    #e. from json_to_recordset('[{"operation":"U","taxCode":1000},{"operation":"U","taxCode":10001}]')
    #as x("operation" text, "taxCode" int);
    
    #create query parameters:
    for col_name in dictColumns.keys():
        
        zag = zag + '\"' + col_name + '\",'
        evals = evals + 'data_array['+str(dictColumns[col_name])+'],' 
        vals = vals + '''\'%s\',''' 
         
        '''
        if col_name in ['model_info','equipment_dict','super_gen']:
            vals = vals + 'json_array_elements(%s),'  
        else:
            vals = vals + 'unnest(array%s),'  
        '''    
        
    zag = zag[:-1]
    vals = vals[:-1]
      
    query = f'''INSERT INTO {table} 
            ({zag}) VALUES ({vals});
            COMMIT;      
            '''%(eval(evals)) 
    
    print('query=',query)
    description='INSERT TABLE auto'            
    cursor_execute(cur,description=description,query=query)     
    
    #close cursor  
    if (cur): cur.close() 
   
    return

def convert_data(data):
    
    import datetime
    dict_months = {}
    months = ['января','февраля','марта','апреля','мая','июня','июля','августа'\
                   ,'сентября','октября','ноября','декабря']
        
    for i,val in enumerate(months, 1):
        dict_months[val] = i
        
    dat = data.split()
    
    if len(dat)==3:
        yy = int(dat[2])  
    else:
        yy = datetime.date.today().year
        
    mm = dict_months[dat[1]]
        
    return datetime.date(yy,mm,int(dat[0]))

def parser_page(url):
    '''
    
    Parameters
    ----------
    url : str - page url for parsing

    Returns
    -------
    html of page

    '''
    try:
        response = requests.get(url, headers={'User-Agent': ua})
        # если ответ успешен, исключения задействованы не будут
        response.raise_for_status()
    
    except HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')  # Python 3.6
    
    except Exception as err:
        print(f'Other error occurred: {err}')  # Python 3.6
    
    else:
        print('Success!')
        
    return BeautifulSoup(response.text, 'html.parser')    


# try connect skillfactory database
    
databaseName = 'skillfactory'

try:
    conn = util_pg.get_conn2(psycopg2,databaseName)
    print(databaseName.upper() + " has connected...")
    
except:
    print("It is unable to connect to the database")
    
cur = conn.cursor()

# if the table AUTO doesn't exist create it    

    
query =  '''
            BEGIN;
            CREATE TABLE IF NOT EXISTS public.auto
            (
                "brand" character varying(20) COLLATE pg_catalog."default",
                "bodyType" character varying(20) COLLATE pg_catalog."default",   
                "car_url" text,
                "color" character varying(12) COLLATE pg_catalog."default",
                "complectation_dict" text,
                "description" text,
                "engineDisplacement" character varying(25) COLLATE pg_catalog."default",
                "enginePower" character varying(25) COLLATE pg_catalog."default",
                "fuelType" character varying(15) COLLATE pg_catalog."default",
                "image" text,
                "mileage" character varying(20),
                "modelDate" char(4),
                "model_name" character varying(20) COLLATE pg_catalog."default",
                "name" character varying(20) COLLATE pg_catalog."default",
                "price" character varying(20),
                "transportTax" character varying(20),
                "numberOfDoors" char(1),
                "parsing_unixtime" character varying(20),
                "priceCurrency" char(3) COLLATE pg_catalog."default",
                "productionDate" char(4),
                "sell_id" character varying(20),
                "vehicleConfiguration" character varying(20) COLLATE pg_catalog."default",
                "vendor" character varying(8) COLLATE pg_catalog."default",
                "vehicleTransmission" character varying(16) COLLATE pg_catalog."default",
                "owners" character varying(10) COLLATE pg_catalog."default",
                "ownership" character varying(20) COLLATE pg_catalog."default",   
                "pts" character varying(10) COLLATE pg_catalog."default",
                "drive" character varying(8) COLLATE pg_catalog."default",
                "wheel" character varying(6) COLLATE pg_catalog."default",
                "state" character varying(20) COLLATE pg_catalog."default",   
                "customs" character varying(15) COLLATE pg_catalog."default",
                "views" character varying(20) COLLATE pg_catalog."default",
                "saleDate" timestamp without time zone,
                "model_info" json,
                "equipment_dict" json,
                "super_gen" json
                );
            COMMIT;
            '''
description='CREATE TABLE auto'            
cursor_execute(cur,query, description)

# create dictionary of table to 

dictColumns = {}
query = '''
    select 
    	c.column_name,
    	-- c.data_type,
    	c.ordinal_position
    from 
    	information_schema.columns c
    where 
    	c.table_schema = 'public'
    	and c.table_name = 'auto'
    	'''

description='select from information_schema.columns of table auto'            
cursor_execute(cur,query, description)
    
for column_name, ordinal_position in cur:
    dictColumns[column_name] = ordinal_position - 1   
    
#print(dictColumns)  

def append_array(data_array, col, value):
    
    '''
    Вариант 2D массива не получился по причине полей JSON -  не смогла побороть
    
    Без них хорошо работает unnest в select'е , но при парсинге напрямую с сайта время не принципиально, все равно спим..
    time.sleep > 5!!! для auto.ru
    
    '''
    #data_array[col] = data_array[col] + [value]    
    data_array[col] = value
    return 

def parsing_auto_ru():
    
    #for i in range(1,100): #now they have 99??? pages at the catalog of moscows cars
    for i in range(0,100000):
        print('Page = ', i)
        #parcing for used cars only for moscow region
        #url = 'http://auto.ru/cars/used/?page=%s?'%(i,)
        url = 'https://auto.ru/moskva/cars/used/?geo_radius=200&page=%s'%(i,)
        
        page = parser_page(url)
        tabl = page.find_all('div',class_='ListingItem__description')
        
        buttons_click = [] # сбрасываем все ссылки со страницы, сюда уже не вернемся!
        
        #create arraay to keep data before insert into table     
        #print('len(dictColumns)=', len(dictColumns))
        #data_array = [[]] * len(dictColumns)

        for j in range(len(tabl)):
            butt_click = tabl[j].find('a',class_='Link ListingItemTitle__link')
            buttons_click.append(butt_click['href'])
           
        for j in range(len(buttons_click)):
            data_array = [''] * len(dictColumns)
            print('J = ', j, len(buttons_click) )
            
            url = buttons_click[j]  
            print(url)
            
            description='select car_url from auto '   
            query = '''select "car_url"
                from auto
                where "car_url" = \'%s\'
                    '''%(url,)
                    
            cur1 = conn.cursor()         
            cursor_execute(cur1,query, description)
            
            if cur1.rowcount > 0:
                continue
            time.sleep(4) #WAIT 10 SEC
            page = parser_page(url) #теперь уже по строке таблицы с авто на странице
            
            append_array(data_array, dictColumns['parsing_unixtime'], time.mktime(datetime.datetime.today().timetuple()))
            append_array(data_array, dictColumns['car_url'], url)
            
            head = page.find('div', class_='CardHead')
            names = head.find('h1').text
            names0 = names.encode('latin1').decode('utf8')
            names = re.findall(r'\b[A-z0-9А-Яа-я\-]+\b',names0)
            
            append_array(data_array, dictColumns['brand'], names[0].upper())
            #pdb.set_trace()
            model_info = re.sub('\'','\"',str({"name": names0[len(names[0])+1:]}))
            append_array(data_array, dictColumns['model_info'], model_info)
            
            try:
                price = head.find('span',class_='OfferPriceCaption__price').text
            except:
                print('Price not found')
                price = '' 
            
            append_array(data_array, dictColumns['price'], re.sub("[^0-9]", "", price))
            
            dtp = page.find_all('div',class_='CardBenefits__item-description')
            try:
                dtp = dtp[1].text.encode('latin1').decode('utf8').replace('\xa0', ' ')
            except:
                print('dtp not found')
                dtp = ''
            
            descript = page.find('div', class_='CardDescription__text')
            try:
                descript = descript.text.encode('latin1').decode('utf8') 
                descript = " ".join(re.findall('[а-яА-ЯёЁA-Za-z0-9\s]+',descript))
            except:
                descript = ''    
            append_array(data_array, dictColumns['description'],descript)
                       
            data = page.find('div', class_='CardHead__infoItem CardHead__creationDate')
            try:
                data = data.text.encode('latin1').decode('utf8')
                data = convert_data(data)
            except:
                data = ''
            append_array(data_array, dictColumns['saleDate'], str(data))
            
            try:
                views = page.find('div', class_='CardHead__infoItem CardHead__views').text.encode('latin1').decode('utf8')
            except:
                print('Views not found')
                views = ''    
            
            append_array(data_array, dictColumns['views'], views)
            # parsing main information
            dict_CarInfo = {"bodytype":"bodyType",
                "color":"color",
                "complectationOrEquipmentCount": "complectation_dict",
                "engine": "enginePower",
                "kmAge": "mileage" ,
                "year": "productionDate",
                "bodytype": "bodyType",
                "productionDate" : 'productionDate',
                "vendor": 'country',
                "transmission": "vehicleTransmission",
                "ownersCount": "owners",
                "owningTime": "ownership",
                "drive": "drive",
                "wheel": "wheel",
                "state": "state",
                "pts": "pts",
                "vin": "vin",
                'warranty':'warranty',
                "transportTax": "transportTax",
                "licensePlate": "licensePlate",
                "electricRange": "electricRange",
                "exchange": "exchange",
                'availability': 'availability',
                "customs": "customs"}
            
            carInfo = page.find('ul',class_='CardInfo')
            li_carinfo = carInfo.find_all('li')
            enum_CardInfo = {}
            for l in range(len(li_carinfo)):
                enum_CardInfo[re.sub('CardInfoRow_','',li_carinfo[l]['class'][1])] = l
            shablon = "Двигатель|Кузов|Цвет|Комплектация|Коробка|Привод|Руль|Состояние|Владельцы|ПТС|Владение|Таможня|дв.|[^А-я0-9\.]"
            #pdb.set_trace()
            for key in enum_CardInfo.keys():
                
                if key in ['year','kmAge','ownersCount','transportTax','vin']:
                    tmp = re.sub("[^0-9A-Z\.]", "", li_carinfo[enum_CardInfo[key]].text.encode('latin1').decode('utf8'))
                elif key == 'bodytype':
                    tmp = re.sub("Кузов|дв|[^А-я\t]", "", li_carinfo[enum_CardInfo[key]].text.encode('latin1').decode('utf8'))   
                    
                else:    
                    tmp = re.sub(shablon, "", li_carinfo[enum_CardInfo[key]].text.encode('latin1').decode('utf8'))
                if dict_CarInfo[key] in dictColumns.keys():
                    append_array(data_array, dictColumns[dict_CarInfo[key]],tmp)
            #parsing technical characteristics 
                       
            url = page.find('a',class_='Link SpoilerLink CardCatalogLink SpoilerLink_type_default')['href']
            
            time.sleep(4)
            
            page = parser_page(url)
            
            catalog_content = page.find_all('div',class_='catalog__content')
            list_labels = catalog_content[0].find_all('dt',class_='list-values__label')
            list_values = catalog_content[0].find_all('dd',class_='list-values__value')
            dict_characteristics = {}
            
            for k in range(len(list_labels)):
                try:
                    dict_characteristics[list_labels[k].text.encode('latin1').decode('utf8')] = list_values[k].text.encode('latin1').decode('utf8')
                except Exception as err:
                    print('list_labels', k)
                    print(f'{err},{list_labels[k].text.encode("latin1").decode("utf8")}')  # Python 3.6   
            
            try:
                tmp =  re.findall('\d+.\d+',dict_characteristics['Объем'])[0] + ' LTR'  
            except Exception as err:
                print('Объем')
                print(f'{err}')
                tmp = 0
                
            append_array(data_array, dictColumns['engineDisplacement'], tmp)
            try:
                tmp =  re.findall('\d+.\d+',dict_characteristics['Мощность'])[0] + ' N12'    
            except Exception as err:
                print('Мощность')
                print(f'{err}')
                tmp = ''
            
            append_array(data_array, dictColumns['enginePower'], tmp)    
            
            append_array(data_array, dictColumns['fuelType'], dict_characteristics['Тип двигателя'])
            
            append_array(data_array, dictColumns['numberOfDoors'], dict_characteristics['Количество дверей'])
            
            equipment_dict = {}
            super_gen = {}
            
            for key in dict_characteristics.keys():
                if key in dict_ru_en.keys():    
                    equipment_dict[dict_ru_en[key]] = dict_characteristics[key]
                else:
                    equipment_dict[key] = dict_characteristics[key]    
                if key in dict_super_gen.keys():
                    super_gen[dict_super_gen[key]] = dict_characteristics[key]   
            
            append_array(data_array, dictColumns['vehicleTransmission'], equipment_dict['transmission'])
            
            append_array(data_array, dictColumns['equipment_dict'], re.sub('\'','\"',str(equipment_dict)))
            
            append_array(data_array, dictColumns['super_gen'], re.sub('\'','\"', str(super_gen)))
            
            #### where are we writing&&&
           
            url = re.sub('specifications','equipment',url)
            time.sleep(4) #WAIT 3 SEC
            page = parser_page(url)
            catalog_content = page.find_all('div',class_='catalog__content')
            try:
                list_labels = catalog_content[0].find_all('li',class_='catalog__package-list-i')
            except:
                continue
            
            list_complectation = ''
            for k in range(len(list_labels)):
                try:
                    list_complectation = list_complectation + ',' + list_labels[k].text.encode('latin1').decode('utf8')
                except Exception as err:
                    print(f'{err}')
                    
            append_array(data_array, dictColumns['complectation_dict'], str(list_complectation[1:]))
            
            '''
            #Control data_array
            print('Control data_array')
            for k in range(1, len(data_array)):
                len_0 = len(data_array[0])
                
                if len(data_array[k]) < len_0:
                    append_array(data_array,k,'None')
                #print(len_0, len(data_array[k]))  
            '''    
            time.sleep(4) #WAIT 10 SEC
            
            for z in range(len(dictColumns)):
                print(z,'----',data_array[z], type(data_array[z]))
                
            insert_db(conn, 'auto', data_array, dictColumns)
            
            
            
        #insert_db(conn, 'auto', data_array, dictColumns)
        
if __name__ == '__main__':
    
    param = 'auto.ru'
    if param == 'auto.ru':
        parsing_auto_ru()
    
    if (conn): conn.close()