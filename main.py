import asyncio
from asyncore import loop
import datetime
import pandas as pd
import motor
from os import listdir
from os.path import isfile, join

class Register:
    def __init__(self,title:str ,timestamp:str, pCut_Motor_Torque:str, pCut_CTRL_Position_controller_Lag_error:str,
        pCut_CTRL_Position_controller_Actual_position:str, pCut_CTRL_Position_controller_Actual_speed:str,
        pSvolFilm_CTRL_Position_controller_Actual_position:str,pSvolFilm_CTRL_Position_controller_Actual_speed:str,
       pSvolFilm_CTRL_Position_controller_Lag_error:str, pSpintor_VAX_speed:str
    ):
        # self.day_of_register = day_of_register
        self.title = title
        self.timestamp = timestamp
        self.pCut_Motor_Torque = pCut_Motor_Torque
        self.pCut_CTRL_Position_controller_Lag_error = pCut_CTRL_Position_controller_Lag_error
        self.pCut_CTRL_Position_controller_Actual_position = pCut_CTRL_Position_controller_Actual_position
        self.pCut_CTRL_Position_controller_Actual_speed = pCut_CTRL_Position_controller_Actual_speed
        self.pSvolFilm_CTRL_Position_controller_Actual_position = pSvolFilm_CTRL_Position_controller_Actual_position
        self.pSvolFilm_CTRL_Position_controller_Actual_speed = pSvolFilm_CTRL_Position_controller_Actual_speed
        self.pSvolFilm_CTRL_Position_controller_Lag_error = pSvolFilm_CTRL_Position_controller_Lag_error
        self.pSpintor_VAX_speed = pSpintor_VAX_speed


# Created to tests
async def insert_one(register_as_dict:dict):
    result = await database.insert_one(register_as_dict)
    print(result)

async def insert_many(registers:list[dict]):
    result = await database.insert_many(registers)
    print(result)


async def read_one_register():
    first_file = database_list[0]
    df = pd.read_csv("{dir}/{file}".format(dir=mypath,file=first_file), delimiter=",")

    # day_of_register = get_datetime(first_file.split(".")[0])

    one_register = Register(first_file.split(".")[0],df.iloc[0][0],df.iloc[0][1],df.iloc[0][2],
    df.iloc[0][3],df.iloc[0][4], df.iloc[0][5],df.iloc[0][6],df.iloc[0][7],df.iloc[0][8])

    print (one_register)
    await insert_one(one_register.__dict__)


async def read_many_registers():
    list_of_register:list[dict] = []
    for file_name in database_list:
        # day_of_register = get_datetime(file_name.split(".")[0])
        title = file_name.split(".")[0]
        # print(file_name)
        data_frame = pd.read_csv("{dir}/{file}".format(dir=mypath,file=file_name), delimiter=",")
        for row in data_frame.iloc:
            timestamp = row[0]
            pCut_Motor_Torque = row[1]
            pCut_CTRL_Position_controller_Lag_error = row[2]
            pCut_CTRL_Position_controller_Actual_position = row[3]
            pCut_CTRL_Position_controller_Actual_speed = row[4]
            pSvolFilm_CTRL_Position_controller_Actual_position = row[5]
            pSvolFilm_CTRL_Position_controller_Actual_speed = row[6]
            pSvolFilm_CTRL_Position_controller_Lag_error = row[7]
            pSpintor_VAX_speed = row[8]

            new_register = Register(title,timestamp,pCut_Motor_Torque,pCut_CTRL_Position_controller_Lag_error,
            pCut_CTRL_Position_controller_Actual_position,pCut_CTRL_Position_controller_Actual_speed,
            pSvolFilm_CTRL_Position_controller_Actual_position,pSvolFilm_CTRL_Position_controller_Actual_speed,
            pSvolFilm_CTRL_Position_controller_Lag_error,pSpintor_VAX_speed)

            list_of_register.append(new_register.__dict__)

    print(len(list_of_register))
    await insert_many(list_of_register)

def get_datetime(file_name:str)->datetime:
    try:
        day_and_month = file_name.split("T")[0]
        (month, day) = day_and_month.split("-")
        print(month)
        print(day)
        if int(day) > 28 and int(month)==2:
            print("PROBLEMA")
        return datetime.datetime(2018, int(month), int(day))
    except Exception as e:
        print(e)
        raise Exception(e)





#---------------------------------------
# print(get_datetime("01-04T184148_000_mode1"))

mypath = "./database_industry_oneyeardata"
database_list = [f for f in listdir(mypath) if isfile(join(mypath, f))]
mongo_access = motor.motor_tornado.MotorClient('localhost', 27017)
database = mongo_access.industrial.component_degradation
try:
    loop = asyncio.get_event_loop()
    loop.run_until_complete(read_many_registers())
except Exception as e:
    print(e)
finally:
    loop.stop()
