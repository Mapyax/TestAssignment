import orjson
import zipfile
from concurrent.futures import ProcessPoolExecutor
from pydantic import BaseModel, Field
from time import time
import logging
import psycopg2

logger = logging.getLogger("logs")
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s | %(message)s")
file_handler = logging.FileHandler('Parsing_logs.txt')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

class Svokvedosn(BaseModel):
    kodOKVED: str = Field(default=None, alias="КодОКВЭД")

class Svokved(BaseModel):
    svOKVEDOsn: Svokvedosn = Field(default=None, alias="СвОКВЭДОсн")

class Region(BaseModel):
    naimRegion: str = Field(default=None, alias="НаимРегион")
    typeRegion: str = Field(default=None, alias="ТипРегион")

class Gorod(BaseModel):
    naimGorod: str = Field(default=None, alias="НаимГород")
    typeGorod: str = Field(default=None, alias="ТипГород")

class Ulica(BaseModel):
    naimUlica: str = Field(default=None, alias="НаимУлица")
    typeUlica: str = Field(default=None, alias="ТипУлица")

class Adresrf(BaseModel):
    region: Region = Field(default=None, alias="Регион")
    kodregion: str = Field(default=None, alias="КодРегион")
    dom: str = Field(default=None, alias="Дом")
    gorod: Gorod = Field(default=None, alias="Город")
    ulica: Ulica = Field(default=None, alias="Улица")
    index: str = Field(default=None, alias="Индекс")

class Svadresul(BaseModel):
    adresRF: Adresrf = Field(default=None, alias="АдресРФ")

class Company_Data(BaseModel):
    svOKVED: Svokved = Field(default=None, alias="СвОКВЭД")
    svAdresUL: Svadresul = Field(default=None, alias="СвАдресЮЛ")

class Company(BaseModel): # pydantic classes to parse needed fields
    name: str
    full_name: str
    inn: str
    kpp: str
    data: Company_Data

    def has_okved_62(self) -> bool: # software development company check
        return (
            self.data and 
            self.data.svOKVED and 
            self.data.svOKVED.svOKVEDOsn and 
            self.data.svOKVED.svOKVEDOsn.kodOKVED.startswith("62")
        )
    
    def is_district(self) -> bool: # registered in the same federal district check
        if not(self.data and
            self.data.svAdresUL and
            self.data.svAdresUL.adresRF and
            self.data.svAdresUL.adresRF.region and
            self.data.svAdresUL.adresRF.region.naimRegion and
            self.data.svAdresUL.adresRF.kodregion): # fields check
            return False
        company_region = self.data.svAdresUL.adresRF.region.naimRegion.upper()
        company_kod_region = self.data.svAdresUL.adresRF.kodregion
        regions_by_district = {
            "ЦЕНТРАЛЬНЫЙ ФЕДЕРАЛЬНЫЙ ОКРУГ": {
                '31': "БЕЛГОРОДСКАЯ",
                '32': "БРЯНСКАЯ",
                '33': "ВЛАДИМИРСКАЯ",
                '36': "ВОРОНЕЖСКАЯ",
                '37': "ИВАНОВСКАЯ",
                '40': "КАЛУЖСКАЯ",
                '44': "КОСТРОМСКАЯ",
                '46': "КУРСКАЯ",
                '48': "ЛИПЕЦКАЯ",
                '77': "МОСКВА",
                '50': "МОСКОВСКАЯ",
                '57': "ОРЛОВСКАЯ",
                '62': "РЯЗАНСКАЯ",
                '67': "СМОЛЕНСКАЯ",
                '68': "ТАМБОВСКАЯ",
                '69': "ТВЕРСКАЯ",
                '71': "ТУЛЬСКАЯ",
                '76': "ЯРОСЛАВСКАЯ"
            },
            "СЕВЕРО-ЗАПАДНЫЙ ФЕДЕРАЛЬНЫЙ ОКРУГ": {
                '29': "АРХАНГЕЛЬСКАЯ",
                '35': "ВОЛОГОДСКАЯ",
                '39': "КАЛИНИНГРАДСКАЯ",
                '10': "КАРЕЛИЯ",
                '11': "КОМИ",
                '47': "ЛЕНИНГРАДСКАЯ",
                '51': "МУРМАНСКАЯ",
                '83': "НЕНЕЦКИЙ",
                '53': "НОВГОРОДСКАЯ",
                '60': "ПСКОВСКАЯ",
                '78': "САНКТ-ПЕТЕРБУРГ"
            },
            "ЮЖНЫЙ ФЕДЕРАЛЬНЫЙ ОКРУГ": {
                '01': "АДЫГЕЯ",
                '30': "АСТРАХАНСКАЯ",
                '34': "ВОЛГОГРАДСКАЯ",
                '08': "КАЛМЫКИЯ",
                '23': "КРАСНОДАРСКИЙ",
                '91': "КРЫМ",
                '61': "РОСТОВСКАЯ",
                '92': "СЕВАСТОПОЛЬ"
            },
            "СЕВЕРО-КАВКАЗСКИЙ ФЕДЕРАЛЬНЫЙ ОКРУГ": {
                '05': "ДАГЕСТАН",
                '06': "ИНГУШЕТИЯ",
                '07': "КАБАРДИНО-БАЛКАРСКАЯ",
                '09': "КАРАЧАЕВО-ЧЕРКЕССКАЯ",
                '15': "СЕВЕРНАЯ ОСЕТИЯ - АЛАНИЯ",
                '26': "СТАВРОПОЛЬСКИЙ",
                '20': "ЧЕЧЕНСКАЯ"
            },
            "ПРИВОЛЖСКИЙ ФЕДЕРАЛЬНЫЙ ОКРУГ": {
                '02': "БАШКОРТОСТАН",
                '43': "КИРОВСКАЯ",
                '12': "МАРИЙ ЭЛ",
                '13': "МОРДОВИЯ",
                '52': "НИЖЕГОРОДСКАЯ",
                '56': "ОРЕНБУРГСКАЯ",
                '58': "ПЕНЗЕНСКАЯ",
                '59': "ПЕРМСКИЙ",
                '63': "САМАРСКАЯ",
                '64': "САРАТОВСКАЯ",
                '16': "ТАТАРСТАН",
                '18': "УДМУРТСКАЯ",
                '73': "УЛЬЯНОВСКАЯ",
                '21': "ЧУВАШСКАЯ"
            },
            "УРАЛЬСКИЙ ФЕДЕРАЛЬНЫЙ ОКРУГ": {
                '45': "КУРГАНСКАЯ",
                '66': "СВЕРДЛОВСКАЯ",
                '72': "ТЮМЕНСКАЯ",
                '86': "ХАНТЫ-МАНСИЙСКИЙ",
                '74': "ЧЕЛЯБИНСКАЯ",
                '89': "ЯМАЛО-НЕНЕЦКИЙ"
            },
            "СИБИРСКИЙ ФЕДЕРАЛЬНЫЙ ОКРУГ": {
                '04': "АЛТАЙ",
                '22': "АЛТАЙСКИЙ",
                '38': "ИРКУТСКАЯ",
                '42': "КЕМЕРОВСКАЯ",
                '24': "КРАСНОЯРСКИЙ",
                '54': "НОВОСИБИРСКАЯ",
                '55': "ОМСКАЯ",
                '70': "ТОМСКАЯ",
                '17': "ТЫВА",
                '19': "ХАКАСИЯ"
            },
            "ДАЛЬНЕВОСТОЧНЫЙ ФЕДЕРАЛЬНЫЙ ОКРУГ": {
                '28': "АМУРСКАЯ",
                '03': "БУРЯТИЯ",
                '79': "ЕВРЕЙСКАЯ",
                '75': "ЗАБАЙКАЛЬСКИЙ",
                '41': "КАМЧАТСКИЙ",
                '49': "МАГАДАНСКАЯ",
                '25': "ПРИМОРСКИЙ",
                '14': "САХА /ЯКУТИЯ/",
                '65': "САХАЛИНСКАЯ",
                '27': "ХАБАРОВСКИЙ",
                '87': "ЧУКОТСКИЙ"
            }
        }
        for regions, distr in zip(regions_by_district.values(), regions_by_district.keys()):
            if (company_kod_region in regions.keys()):
                kod_distr = distr
            if (company_region in regions.values()):
                region_distr = distr
        return kod_distr == region_distr

    def generate_adres(self) -> str: # address assembly
        adres = ''
        if (self.data and
            self.data.svAdresUL and
            self.data.svAdresUL.adresRF): # fields check
            rf_address = self.data.svAdresUL.adresRF
            if (rf_address.index):
                adres += rf_address.index + ', ' # index
            if (rf_address.region and
                rf_address.region.naimRegion):
                adres += rf_address.region.typeRegion + ' ' + rf_address.region.naimRegion + ', ' # region
            if (rf_address.gorod and
                rf_address.gorod.naimGorod):
                adres += rf_address.gorod.typeGorod + rf_address.gorod.naimGorod + ', ' # city
            if (rf_address.ulica and
                rf_address.ulica.naimUlica):
                adres += rf_address.ulica.typeUlica + rf_address.ulica.naimUlica + ', ' # street
            if (rf_address.dom):
                adres += rf_address.dom # building
        return adres

def insert_data_to_db(data: list) -> None:
    conn = psycopg2.connect(
        database='postgres', user='postgres', password='123', host='localhost' # connect to your database
    )
    cursor = conn.cursor()
    # logger.info("Connected to DB")
    try:
        cursor.execute("INSERT INTO EGRUL62 (name, full_name, okved, inn, kpp, adres) VALUES (%s, %s, %s, %s, %s, %s)", data) # basic sql query
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error: {e}")
    finally:
        cursor.close()
        conn.close()

def process_companies(companies: list):
    for company in companies:
                    parsed_data = Company(**company) # pydantic fields parsing
                    if (parsed_data.has_okved_62() and parsed_data.is_district()): # software dev check
                        insert_list = [parsed_data.name,
                                        parsed_data.full_name, 
                                        parsed_data.data.svOKVED.svOKVEDOsn.kodOKVED, 
                                        parsed_data.inn, 
                                        parsed_data.kpp,
                                        parsed_data.generate_adres()] # fields to insert in database
                        insert_data_to_db(insert_list)

def chunk_generation(zip_filename: str, chunk_size: int) -> list:
    with zipfile.ZipFile(zip_filename, 'r') as archive:
        logger.info(f"Opened archive: {zip_filename}")
        chunks = []
        for i in range(0, len(archive.namelist()), chunk_size):
            chunks.append(archive.namelist()[i:i + chunk_size]) # divides archive files into chunks
    logger.info("Chunks generated")
    return chunks

def parse_json_file(files: list) -> None:
    with zipfile.ZipFile('egrul.json.zip', 'r') as archive: # since it is backend project just type in your zip file name here, it's much more consuming to stream it into processes
        for file_name in files:
            with archive.open(file_name) as extracted_file: # process openning every file in a given chunk
                companies = orjson.loads(extracted_file.read()) # json formatting
                process_companies(companies)
    logger.info("Chunk has been parsed")

def main():
    zip_filename = 'egrul.json.zip'
    chunk_size = 20 # feels optimal on my pc in terms of execution speed and memory usage(significantly depends on your pc setup especially cpu and storage device)
    logger.info("Start")

    chunks = chunk_generation(zip_filename, chunk_size)
    
    with ProcessPoolExecutor() as executor:
        executor.map(parse_json_file, chunks) # feeding processes with chunks to parse
        
    logger.info("Executed")

if __name__ == "__main__":
    start_time = time()
    main()
    print(f"Время выполнения: {time() - start_time} секунд")