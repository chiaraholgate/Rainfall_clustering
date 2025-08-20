'''

This script identifies "clusters" of "heavy" rainfall.

Heavy rainfall = daily rainfall above the climatological monthly 90th percentile.

Clusters = 7-day rolling count of heavy rain days, that are above the climatological 7-day rolling count of such days.

Climatological baseline is 1980-2020 in both cases.

Using 1x1deg daily gridded precipitation datasets.

Takes ~3hrs per 5deg longitude slice using the 1deg CPC dataset using 8 CPUs, about 20GB memory.

27/6/25: I've moved part of this code to "Identify_heavy_rain_days.py" and run separately, since it only needs to be run once per dataset.

'''

import xarray as xr
import pandas as pd
import numpy as np
import glob
import os
# import dask.distributed
import sys

# https://coecms-training.github.io/parallel/dask-batch.html
# To enable Dask to use the resources requested by the run script you should start a Dask client. 
# if __name__ == '__main__':
#     dask.distributed.Client(
#         n_workers = int(os.environ['PBS_NCPUS']),
#         memory_limit = int(os.environ['PBS_VMEM']) / int(os.environ['PBS_NCPUS']),
#         local_directory = os.path.join(os.environ['PBS_JOBFS'], 'dask-worker-space')
#     )


#########
# Take starting longitude from bash argument, noting that the arguments will be read in as floats (for numbers) and strings (for text) by default
if len(sys.argv) > 0:
    first_lon = float(sys.argv[1])
    last_lon = float(sys.argv[2])
    deg_step = float(sys.argv[3])
    ds = str(sys.argv[4]) # should be one of ['CPC','GPCC','ERA5','MSWEP']
#########
    

# datasets = ['CPC','GPCC','ERA5','MSWEP']

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

# Directory of daily rainfall percentiles
pctl_dir = '/g/data/ng72/cxh603/Global_drought_termination/Percentiles/'

# Main output directory
out_dir = '/g/data/ng72/cxh603/Global_drought_termination/Heavy_rainfall/'

# Temporary directory for longitudinal sliced output
out_dir_chunks = '/scratch/ng72/cxh603/'


# Rolling window length in days
window = 7 # days

# Daily precipitation percentile you are considering as heavy rainfall, and its baseline period
pctl = 90
baseline_period_start, baseline_period_end = 1980, 2020


# Rolling count of heavy rainfall days percentile
q = 0.9


'''
Compute for each dataset
'''

# for ds in datasets[3:]: 
# ds = datasets[0]
    
data_files = sorted(glob.glob(f'{data_dir[ds]}/{data_rootname[ds]}*.nc'))

data_start_year, data_end_year = data_files[0][-7:-3], data_files[-1][-7:-3]


'''
Compute and save the rolling count above the corresponding percentile threshold, done per longitude slice 
'''

rolling_count_fname = out_dir+ds+'.1DD.rolling_count_'+str(window)+'day_above.moy_p'+str(pctl)+'.baseline_'+str(baseline_period_start)+'-'+str(baseline_period_end)+'.'+str(data_start_year)+'-'+str(data_end_year)+'.nc' 
fh = xr.open_dataset(rolling_count_fname)
rolling_count_in = fh.rain
fh.close()



start_lon = first_lon    
lon_step = deg_step
# max_lon = start_lon + lon_step*16
max_lon = last_lon


for lon_i in np.arange(start_lon,max_lon,lon_step): 

    rolling_count = rolling_count_in.sel(lon = slice(lon_i, lon_i + lon_step)).load()
    grouped = rolling_count.groupby('MMDD')


    app_quantile = []
    for g in list(grouped.groups.keys()):
    
        datasets_sections = []
        for i in range(len(grouped[g].time)):        
            data_section = rolling_count.sel(time=slice(grouped[g].time[i], grouped[g].time[i] + np.timedelta64(6,'D')))      
            # The last year's MMDD will not have time+6, as there's no data to include. Don't include this last
            # MMDD, as it will always give an artifically low count, given there's only 1 day in the slice.
            if len(data_section.time) > 1:
                datasets_sections.append(data_section)
        combined = xr.concat(datasets_sections, dim='time').chunk({'time':-1, 'lat':'300mb', 'lon':'300mb'})
        group_quantile = combined.quantile(q, dim='time', skipna=True)
        app_quantile.append(group_quantile)


    arr_quantile = xr.concat(app_quantile, dim='MMDD').reindex({'MMDD':list(grouped.groups.keys())}).chunk({'lat':'300mb', 'lon':'300mb'})
    
    rolling_count_above = rolling_count.groupby('MMDD').where(rolling_count.groupby('MMDD') > arr_quantile)
  
    outfile = out_dir_chunks+ds+'.1DD.rolling_count_'+str(window)+'day_above.'+str(window)+'day_p'+str(int(q*100))+'.baseline_'+str(baseline_period_start)+'-'+str(baseline_period_end)+'.'+str(data_start_year)+'-'+str(data_end_year)+'.'+str(lon_i)+'-'+str(lon_i+lon_step)+'deg_lon.nc'  
    
    encoding = {
        'rain': { # Variable name
            'zlib': True, # Turn on compression
            'shuffle': True, # Turn on shuffle filter
            'complevel': 4, # Compression amount (0-9), 4 is a good choice
        }
    }
    ## WILL OVERWRITE
    rolling_count_above.to_netcdf(outfile, encoding=encoding, compute=True, mode="w")
    # rolling_count_above.to_netcdf(outfile, compute=True, mode="w")
    rolling_count_above.close()

