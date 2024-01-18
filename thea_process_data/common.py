from multiprocessing import Pool
import os
import s3fs
import logging
import numpy as np
import pyarrow.parquet as pq
from datetime import datetime, timedelta
from multiprocessing.dummy import Pool as ThreadPool
from netCDF4 import Dataset
from config import Config
from dateutil.relativedelta import relativedelta
from itertools import repeat
import re
import toml
import logging

#  to do list 

    # revise  it to inheritance ---check 
    # get rid of magic number --- check
    # change config to property() -- check
    # make it as package using cookiecutter

class BaseDataProcessor:
    def __init__(self, base_path_cfg,save_base_path_cfg,cycles_cfg,steps_cfg,num_steps_cfg,num_members):
        self._base_path_cfg = base_path_cfg
        self._save_base_path_cfg = save_base_path_cfg
        self._cycles_cfg = cycles_cfg
        self._steps_cfg = steps_cfg
        self._num_steps_cfg = num_steps_cfg
        self._num_members = num_members
        
        logging.basicConfig(filename= f'{self.__class__.__name__.lower()}_missing_info_gefs.log', level=logging.ERROR)
        
    @property
    def base_path_cfg(self):
        return self._base_path_cfg

    @property
    def save_base_path_cfg(self):
        return self._save_base_path_cfg

    @property
    def cycles_cfg(self):
        return self._cycles_cfg

    @property
    def steps_cfg(self):
        return self._steps_cfg

    @property
    def num_steps_cfg(self):
        return self._num_steps_cfg

    @property
    def num_members(self):
        return self._num_members
    
    def construct_path(self, date, time, step , num_members):
        mem_list = []
        for mem in num_members:
            
            valid_date = date + timedelta(hours = int(step)+ int(time))
            file_name = (
                f"step={step}_validdate="
                + datetime.strftime(valid_date, "%Y%m%d")
                + "T"
                + datetime.strftime(valid_date, "%H%M%S")
                + "Z.parquet"
            )
            path = os.path.join(
                self.base_path_cfg,
                "date=" + datetime.strftime(date, "%Y%m%d"),
                f"time={time}",
                "mem=" + "{:02}".format(mem),
                file_name,
            )
            mem_list.append(path)
        return mem_list
    
    def check_mem(self,mem):
        config = Config().read_config()
        s3 = s3fs.S3FileSystem(anon=False,key=config['aws']['access_key_id'],secret=config['aws']['secret_access_key'])  
        date = re.search(r'date=(\d{8})', mem)
        time = re.search(r'time=(\d{2})', mem)
        mem_ = re.search(r'mem=(\d{2})', mem)
        
        date_time= date.group(1)
        time_time= time.group(1)
        mem_time = mem_.group(1)
        
        if not s3.exists(f"s3://prizm-glow/wx-data-intake/eps/date={date_time}"):
            msg = f"date={date_time} is missing."
            with open('missing_gefs.log','a') as f:
                f.write(msg + '\n')
            return None

        if not s3.exists(f"s3://prizm-glow/wx-data-intake/eps/date={date_time}/time={time_time}"):
            msg =f" date={date_time}/time={time_time} is missing."
            with open('missing_gefs.log','a') as f:
                f.write(msg + '\n')
            return None

        if not s3.exists(f"s3://prizm-glow/wx-data-intake/eps/date={date_time}/time={time_time}/mem={mem_time}"):
            msg = f"date={date_time}/time={time_time}/mem={mem_time} is missing "
            with open('missing_gefs.log','a') as f:
                f.write(msg + '\n')
            return None
        return mem
        
    
    def read_data(self,mem):
        try:
            config = Config().read_config()
            s3 = s3fs.S3FileSystem(anon=False,key=config['aws']['access_key_id'],secret=config['aws']['secret_access_key'])  
 
            # print(mem)
            mem = self.check_mem(mem)
            data = pq.read_table(mem,filesystem=s3)
            mem_data = np.zeros((13, 92, 151))
            print("now processing:",  mem)

            for i, variable in enumerate(config['variables']['list']):
                variable_data = np.array(data[variable]).reshape(92, 151)
                
                # Check for NaN values
                if np.isnan(variable_data).any():
                    msg = f"Warning: NaN values found in variable '{variable}' for file {mem}"
                    with open('nan_values.log','a') as f:
                        f.write(msg + '\n')
                    
                mem_data[i, :, :] += variable_data
                
            u100 =np.array(data['100u'])
            v100 =np.array(data['100v'])
            u10 = np.array(data['10u'])
            v10 = np.array(data['10v'])
        
            ws100 = np.sqrt(u100**2 + v100**2)  
            ws10 = np.sqrt(u10**2 + v10**2) 
            
            mem_data[11, :, :] = ws100.reshape(92,151)
            mem_data[12, :, :] = ws10.reshape(92,151)
            
            return mem_data
        except Exception as e:
            print(f"The file does not exist in S3: {mem}")
            print("no such file in s3! recording info...")
            
            error_message = f"missing file: {mem}"
            print(error_message)
            logging.error(error_message)
            return None

    def save_em(self, em,step,date,time):
        config = Config().read_config()
        output_dir = os.path.join(
            self.save_base_path_cfg,
            "date=" + datetime.strftime(date, "%Y%m%d"),
            f"time={time}",
            "mem=00",
        )
        os.makedirs(output_dir,exist_ok=True)
        valid_date = date + timedelta(hours = int(step)+ int(time))
        file_name = (
            f"step={step}_validdate="
            + datetime.strftime(valid_date, "%Y%m%d")
            + "T"
            + datetime.strftime(valid_date, "%H%M%S")
            + "Z.nc"
        )
        output_path = os.path.join(output_dir, file_name)
        print(output_path)

        netcdf_file = Dataset(output_path, "w", format = "NETCDF4")

        # Create dimensions
        netcdf_file.createDimension('latitude', 92)
        netcdf_file.createDimension('longitude', 151)

        for i, var in enumerate(config['variables']['list_windspeed']):
            nc_var = netcdf_file.createVariable(var,"f4", ('latitude','longitude'))
            nc_var[:] = em[i,:,:]
        netcdf_file.close()
        
    def process_step(self,args):

        step = args[0]
        date = args[1]
        time = args[2]
        # config = Config().read_config()
        mem_list = self.construct_path(date = date, time = time,  step = step, num_members = self.num_members)
     
        with ThreadPool(8) as mem_pool:         
            ens_sum = mem_pool.map(self.read_data, mem_list)

        ens_sum = np.array(ens_sum)
        em = np.sum(ens_sum, axis = 0) / ens_sum.shape[0]
        self.save_em(em, step, date, time)

    def process_date(self,date):
        # config = Config().read_config()
        for time in self.cycles_cfg:
            date_list = [date for date in repeat(date,  self.num_steps_cfg)] 
            # print(date_list)
            time_list = [time for time in repeat(time,  self.num_steps_cfg )]
            # print(time_list)
            args = list(zip(self.steps_cfg, date_list,  time_list))
            # print(args)
            with ThreadPool(8) as step_pool:
                step_pool.map(self.process_step, args)
                
    def main(self,start_date):
        date_start = datetime.strptime(start_date, '%Y%m%d')
        date_end = date_start + relativedelta(years = 1)
        print(date_start, date_end )
        date_list = []
        while date_start != date_end:
            date_list.append(date_start)
            date_start = date_start + timedelta(days = 1)  

        
        with Pool(4) as date_pool:
            date_pool.map(self.process_date, date_list)



class Config:
    def __init__(self, config_file="config.toml"):
        self.config_file = config_file
        
    def read_config(self):
        default_config = {
            "aws": {
                "access_key_id": "default_access_key",
                "secret_access_key": "default_secret_key"
            },
            "variables": {
                "list": ["100u", "100v", "10u", "10v", "2d", "2t", "sp", "sro", "ssrd", "tcc", "tp"],
                "list_windspeed": [ "100u", "100v", "10u", "10v", "2d", "2t", "sp", "sro", "ssrd", "tcc", "tp", "100ws", "10ws"]
            },
            "cycles": {
                "list_eps":  ["00", "12"],
                "list_gefs": ["00", "06", "12", "18"]
            },
            "steps": {
                "list_eps": ["000", "001", "002", "003", "004", "005", "006", "007", "008", "009", "010", "011", "012", "013", "014", "015", "016", "017", "018", "019", "020", "021", "022", "023", "024", "025", "026", "027", "028", "029", "030", "031", "032", "033", "034", "035", "036", "037", "038", "039", "040", "041", "042", "043", "044", "045", "046", "047", "048", "049", "050", "051", "052", "053", "054", "055", "056", "057", "058", "059", "060", "061", "062", "063", "064", "065", "066", "067", "068", "069", "070", "071", "072", "073", "074", "075", "076", "077", "078", "079", "080", "081", "082", "083", "084", "085", "086", "087", "088", "089", "090", "093", "096", "099", "102", "105", "108", "111", "114", "117", "120", "123", "126", "129", "132", "135", "138", "141", "144", "150", "156", "162", "168", "174", "180", "186", "192", "198", "204", "210", "216", "222", "228", "234", "240", "246", "252", "258", "264", "270", "276", "282", "288", "294", "300", "306", "312", "318", "324", "330", "336", "342", "348", "354", "360"],
                "list_gefs": ['000', '003', '006', '009', '012', '015', '018', '021']
            },
            "paths": {
                "base_path_eps": "s3://prizm-glow/wx-data-intake/eps/",
                "save_base_path_eps": "/net/airs1/storage/projects/Unet/em_eps",
                "base_path_gefs":"s3://prizm-glow/wx-data-intake/gefs/",
                "save_base_path_gefs": "/net/airs1/storage/projects/Unet/gefs_em"
            },
            "members":{
                "eps": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50],
                "gefs": [0,1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31]
            },
            "num_steps":{
                "eps": "145",
                "gefs": "105"
            }
    }
        try: 
            with open(self.config_file, "r") as f:
                default_config.update(toml.load(f))
                
        except FileNotFoundError:
                logging.error("Config file not found. Using default config.")
                
        return default_config  