'''

This script computes calendar-month percentiles on 1x1deg daily gridded precipitation datasets.

Percentiles are calculated over the baseline period 1980-2020, considering only days with rainfall above a minimum defined threshold.


'''

import xarray as xr
import glob
import pandas as pd
import numpy as np
import os
import dask.distributed

datasets = ['CPC','GPCC','ERA5','MSWEP']

dir_out = '/g/data/ng72/cxh603/Global_drought_termination/Weather_features/'

data_dir = {'CPC':'/g/data/ia39/aus-ref-clim-data-nci/frogs/data/1DD_V1/CPC_v1.0/',
            'GPCC':'/g/data/ia39/aus-ref-clim-data-nci/frogs/data/1DD_V1/GPCC_FDD_v2022/',
            'ERA5':'/g/data/su28/ERA5/daily/tp/',
            'MSWEP':'/g/data/ng72/cxh603/Data/MSWEP/'}

data_rootname = {'CPC':'CPC.1DD.',
            'GPCC':'full_data_daily_v2022_10_',
            'ERA5':'tp_era5_oper_sfc_merge_1deg_daily_',
            'MSWEP':'mswep_v280_day_1x1deg_'}

var_name = {'CPC':'rain',
            'GPCC':'rain',
            'ERA5':'tp',
            'MSWEP':'precipitation'}

landmasked = {'CPC':True,
            'GPCC':True,
            'ERA5':False,
            'MSWEP':False}

landmask_file = {'CPC':'',
            'GPCC':'',
            'ERA5':'/g/data/su28/ERA5/daily/ldmk/landmask_r360x180.nc',
            'MSWEP':'/g/data/ng72/cxh603/Data/MSWEP/landmask.nc'}

landmask_var_name = {'CPC':'',
            'GPCC':'',
            'ERA5':'topo',
            'MSWEP':'topo'}

out_dir = '/g/data/ng72/cxh603/Global_drought_termination/Percentiles/'

percentiles = [0.1, 0.15, 0.5, 0.9, 0.95, 0.98]

baseline_period_start, baseline_period_end = 1980, 2020

min_rain = 0.5 # mm/day

encoding = {
    'quantile': { # Variable name
        'zlib': True, # Turn on compression
        'shuffle': True, # Turn on shuffle filter
        'complevel': 4, # Compression amount (0-9), 4 is a good choice
    }
}


# https://coecms-training.github.io/parallel/dask-batch.html
# To enable Dask to use the resources requested by the run script you should start a Dask client. 
if __name__ == '__main__':
    dask.distributed.Client(
        n_workers = int(os.environ['PBS_NCPUS']),
        memory_limit = int(os.environ['PBS_VMEM']) / int(os.environ['PBS_NCPUS']),
        local_directory = os.path.join(os.environ['PBS_JOBFS'], 'dask-worker-space')
    )
    

    # for ds in datasets[3:]: 
    ds = datasets[2]    

    def data_preprocess(fh):
        rain = fh[var_name[ds]]
        return rain
        
    data_files = sorted(glob.glob(f'{data_dir[ds]}/{data_rootname[ds]}*.nc'))

    data_start_year, data_end_year = data_files[0][-7:-3], data_files[-1][-7:-3]

    ds_preprocess = xr.open_mfdataset(data_files,
                                      preprocess = data_preprocess,
                                      parallel = True).sel(time=slice('1/1/'+str(baseline_period_start),'31/12/'+str(baseline_period_end))).chunk({"time": -1, 'lat': '300mb', 'lon': '300mb'})
    
    if landmasked[ds] == False:
        # Land mask the data before using it
        landmask = xr.open_dataset(landmask_file[ds])[landmask_var_name[ds]]
        ds_preprocess = ds_preprocess.where(landmask == 1)
    
    
        
        
    for q in percentiles[2:]: # 10th and 15th already done for ERA5 - check they are correct though!
    
        dataarrays = []
            
        for month in range(1,13):       
            
            precip = ds_preprocess[var_name[ds]].sel(time=ds_preprocess.time.dt.month == month).chunk({"time": -1, 'lat': '300mb', 'lon': '300mb'})       
            
            # Could automate this by looping through possible unit options
            if ds == 'ERA5':
                precip = precip*1000 # to convert from metres to millimetres
                
            qn = precip.where(precip >= min_rain).quantile(q, 'time', skipna=True).chunk('auto')
            dataarrays.append(qn)       
        
        qn_combined = xr.concat(dataarrays, dim='month').to_dataset()
    
        attributes = {'global attributes':"Created "+str(pd.to_datetime("today")),
                                          'Base data':data_dir[ds],
                                          'Percentile method':'xarray quantile'}
        
        qn_combined.attrs = attributes
        outfile = out_dir+ds+'.1DD.baseline_'+str(baseline_period_start)+'-'+str(baseline_period_end)+'.moy_p'+str(int(q*100))+'.'+str(data_start_year)+'-'+str(data_end_year)+'.nc'      
    
        ## WILL OVERWRITE
        qn_combined.to_netcdf(outfile, encoding=encoding, mode="w")