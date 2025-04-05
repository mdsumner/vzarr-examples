
## I have a number of these monthly merged files on disk 
#catalog <- "https://www.oceancolour.org/thredds/catalog/cci/v6.0-release/geographic/monthly/all_products/1997/catalog.html?dataset=CCI_ALL-v6.0-Geographic%2Fmonthly%2Fall_products%2F1997%2FESACCI-OC-L3S-OC_PRODUCTS-MERGED-1M_MONTHLY_4km_GEO_PML_OCx_QAA-199709-fv6.0.nc"
#qu <- "https://www.oceancolour.org/browser/get.php?date=2024-01-01&product=chlor_a&period=monthly&format=netcdf&mapping=GEO&version=6"
#u <- "https://www.oceancolour.org/thredds/fileServer/cci/v6.0-1km-release/geographic/monthly/chlor_a/2024/ESACCI-OC-L3S-CHLOR_A-MERGED-1M_MONTHLY_4km_GEO_PML_OCx-202401-fv6.0.nc"
#u       "https://www.oceancolour.org/thredds/fileServer/cci/v6.0-1km-release/geographic/2013/ESACCI-OC-L3S-OC_PRODUCTS-MERGED-1D_DAILY_1km_GEO_PML_OCx_QAA-20130108-fv6.0_1km.nc
        # https://www.oceancolour.org/thredds/fileServer/cci/v6.0-1km-release/geographic/2013/ESACCI-OC-L3S-OC_PRODUCTS-MERGED-1M_MONTHLY_4km_GEO_PML_OCx_QAA-201308-fv6.0.nc
# https://www.oceancolour.org/thredds/fileServer/cci/v6.0-release/geographic/monthly/all_products/2013/ESACCI-OC-L3S-OC_PRODUCTS-MERGED-1M_MONTHLY_4km_GEO_PML_OCx_QAA-201308-fv6.0.nc

from virtualizarr import open_virtual_mfdataset
from pathlib import Path
from re import search 
from concurrent.futures import ThreadPoolExecutor
import glob
#files = ["ESACCI-OC-L3S-OC_PRODUCTS-MERGED-1M_MONTHLY_4km_GEO_PML_OCx_QAA-201308-fv6.0.nc", "ESACCI-OC-L3S-OC_PRODUCTS-MERGED-1M_MONTHLY_4km_GEO_PML_OCx_QAA-201309-fv6.0.nc"]
files = glob.glob("*.nc")
#files = files[0:3]
ds = open_virtual_mfdataset(files, parallel = ThreadPoolExecutor, loadable_variables = ["lon", "lat", "time", "crs"])

def local_to_url(old_local_path: str) -> str:
    filename = Path(old_local_path).name
    year = search("[0-9]{4}", filename).group()
    new_url = f"https://www.oceancolour.org/thredds/fileServer/cci/v6.0-release/geographic/monthly/all_products/{year}"
    
    return str(f"{new_url}/{filename}")
  
  
## this fails
#ds.virtualize.rename_paths(local_to_url)

## but this works
#data = ds["chlor_a"].data

# data.rename_paths(local_to_url)
# ManifestArray<shape=(2, 4320, 8640), dtype=float32, chunks=(1, 270, 270)>

## so 
from virtualizarr.manifests import ManifestArray
new_ds = ds.copy()
new = str ## emulate virtualizarr function
for var_name in new_ds.variables:
        data = new_ds[var_name].data
        if isinstance(data, ManifestArray):
            print(var_name)
            new_ds[var_name].data = data.rename_paths(new=new)

from numpy import float64

## and we have to do this
new_ds = new_ds.assign_coords(lon = float64(new_ds.lon.values), lat = float64(new_ds.lat.values))
## and/or this
new_ds.attrs = {}  ## it's some float32(-90) value

new_ds.virtualize.to_kerchunk("../vzarr-examples/essacci-monthly.json", format = "json")
new_ds.virtualize.to_kerchunk("../vzarr-examples/essacci-monthly.parquet", format = "parquet")
##new_ds.virtualize.to_icechunk("../vzarr-examples/essacci-monthly.zarr")

