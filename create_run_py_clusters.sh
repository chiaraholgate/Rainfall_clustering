################################################################################################

# Create and submit PBS scripts that run Identify_clusters.py for different slices of longitude.

################################################################################################



#!/bin/bash

code_dir="/home/603/cxh603/21CW/Global_drought_termination/Processing/"
pctl_dir="/g/data/ng72/cxh603/Global_drought_termination/Percentiles/"

mkdir -p ${code_dir}"batch_by_lon/" || { echo "error: cannot create folder $code_dir"; exit 1; }


# datasets=('CPC' 'GPCC' 'ERA5' 'MSWEP')
datasets=('GPCC' 'ERA5' 'MSWEP')
lon_step=5
no_lon_increments=10 # approx 16*5deg of lon will run within 48hr walltime, but do fewer to allow for rain_above part of script to run 
for dataset in "${datasets[@]}"; do
    
    # Find range of longitudes using the percentile grid as the reference
    fname=${pctl_dir}${dataset}".1DD.baseline_*.moy_p90.*.nc"
    ncdump -v lon $fname | sed -e '1,/data:/d' -e '$d' | tail -n +2 > lons
    lon_start=$(echo $(head -n 1 lons | tr -d 'lon=' | cut -d',' -f 1))
    linelen=$(wc -w <<< "$(tail -n 1 lons | tr -d ';')")
    lon_end="$(echo $(tail -n 1 lons | tr -d ';') | cut -d',' -f $linelen | tr -d '[:space:]')"
    
    max_lon=${lon_end}

    # Create enough longitude slices to cover all lons
    for i in {0..10}; do
    first_lon=$(echo "${lon_start}+${lon_step}*${no_lon_increments}*$i" | bc)
    if [[ $(echo "${first_lon} > ${max_lon}" | bc) -eq 1 ]]; then
        break
    fi
    last_lon=$(echo "${lon_start}+${lon_step}*${no_lon_increments}*(($i+1))" | bc)
    if [[ $(echo "${last_lon} >= ${max_lon}" | bc) -eq 1 ]]; then
        last_lon=${max_lon}
    fi

    submission_file="${code_dir}batch_by_lon/run_clusters_${dataset}_${first_lon}-${last_lon}deg_lon.sh"

    cat > "$submission_file" << EOF
#!/bin/bash

#PBS -P ng72
#PBS -N clusters_${dataset}_${first_lon}-${last_lon}
#PBS -q normal
#PBS -l walltime=48:00:00
#PBS -l ncpus=8
#PBS -l mem=20GB
#PBS -l jobfs=0GB
#PBS -l storage=gdata/tp28+gdata/hh5+gdata/w28+gdata/w35+gdata/ks32+gdata/eg3+gdata/rt52+gdata/zv2+gdata/w97+gdata/fs38+gdata/ia39+gdata/ng72+gdata/xp65+gdata/su28+gdata/ct11+scratch/ng72+gdata/jt48

module use /g/data3/xp65/public/modules
module load singularity
module load conda/analysis3

# I am getting seg faults, so try this
ulimit -s unlimited

set -eu

python ${code_dir}Identify_clusters.py ${first_lon} ${last_lon} ${lon_step} ${dataset}
EOF

done
done


# for script in ${code_dir}"batch_by_lon/"run_clusters_*deg_lon.sh; do
#     echo "Submitting $script..."
#     qsub "$script"
# done
