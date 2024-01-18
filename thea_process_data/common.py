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


