"""Main module."""
import sys
from data_processor import DataProcessor
from config import Config

if __name__ == "__main__":
    
    # cfg = Config().read_config()
    # base_path_cfg = cfg['paths']['base_path_eps']
    # save_base_path_cfg = cfg['paths']['save_base_path_eps']
    # cycles_cfg = cfg["cycles"]['list_eps']
    # steps_cfg = cfg['steps']['list_eps']
    # num_steps_cfg = cfg['num_steps']['eps']
    # num_members = cfg['members']['eps']
    
    cfg = Config().read_config()
    base_path_cfg = cfg['paths']['base_path_gefs']
    save_base_path_cfg = cfg['paths']['save_base_path_gefs']
    cycles_cfg = cfg["cycles"]['list_gefs']
    steps_cfg = cfg['steps']['list_gefs']
    num_steps_cfg = cfg['num_steps']['gefs']
    num_members = cfg['members']['gefs']
    
    eps = DataProcessor(base_path_cfg,save_base_path_cfg,cycles_cfg,steps_cfg,num_steps_cfg,num_members)
    eps.main(sys.argv[1])
    
