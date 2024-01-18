from base_data_processor import BaseDataProcessor


class DataProcessor(BaseDataProcessor):
    def __init__(self,base_path_cfg, save_base_path_cfg, cycles_cfg, steps_cfg, num_steps_cfg,num_members):

        # self.base_path_cfg = base_path_cfg
        # self.save_base_path_cfg = save_base_path_cfg
        # self.cycles_cfg = cycles_cfg
        # self.steps_cfg = steps_cfg
        # self.num_steps_cfg = num_steps_cfg
        # self.num_numbers = num_members
        super().__init__(base_path_cfg, save_base_path_cfg, cycles_cfg, steps_cfg, num_steps_cfg,num_members)
 

