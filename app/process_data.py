# This notebook is used to for historical weather data gathering . We are using air temperature metric for the purpose of demonstration.
# Steps:
# 1. Load 200 files in netCDF format from S3
# 2. Load files as cubes filterred by air temperature
# 3. Filter data further by no of dimensions. Currently we are filtering for 2 dinesional data nd can be extended to 3 and 4 as well.
# 4. Read x, y projections and convert it to latitude, longitude coordinates
# 5. Read time values and convert to standard time using numpy
# 6. Read temperature data points for each latitude, longitude, time. Conver from K to celcius
# 7. Write to CSV format and upload to S3 bucket. Files are stored by "air temperature"_date format
#
# The process runs as a concurrent.futures.ThreadPoolExecutor for pricessing multiple files.
# Note:This can be enhanced to use parallel processing using DASK api's as the cube data are returned as lazy dask array.
#
# The CSV data is loaded into AWS QuickSight for querying in tabular format/visualizations.


import os
import json
import datetime
import time
import boto3
import urllib.request
import csv
import concurrent.futures
# pyproj package used to convert the x,y projections from netCDF files to lattitude longitude format.
# converting from projection coordinates to geographic
import pyproj
import numpy as np
import iris


# Global Params
# Public bucket from MET Office
# Bucket storing csv files for further processing in the pipeline
csv_output_bucket = 'mogreps-uk-csv-output'
s3 = boto3.client('s3')


# Lambda function entry point
def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(
        event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        # download the file to the local tmp folder
        tmp_file_name = os.path.join("/tmp", key)
        download_locally(bucket, key, tmp_file_name)

        listofcubes = iris.load(tmp_file_name)
        # Print the cube stats
        for cube in listofcubes:
            print_cube(cube, print_cube=False, print_stats=True)

        # Filter data for a 2 dimensional cube
        two_dim_subset_cubes = listofcubes.extract(
            iris.Constraint(cube_func=filter_cube)
        )

        # Create multiple threads and run in background as futures object to process multiple cubes.
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
            # Start the load operations and mark each future with its URL
            future_output = {executor.submit(
                process_cube, cube, "celsius", 10, 200): cube for cube in two_dim_subset_cubes}
            for future in concurrent.futures.as_completed(future_output):
                cube = future_output[future]
                try:
                    data = future.result()
                    print("data:{}".format(data))
                    os.remove(tmp_file_name)
                except Exception as exc:
                    print('{} generated an exception: {}'.format(cube, exc))
                    raise exc
                else:
                    print('{} cube is {} data'.format(cube, data))

        return "File::{} is being processed in the background".format(key)
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e


#  HELPER METHODS
# Download netCDF file locally to process

# Print cube details including statistics.
def print_cube(cube, print_cube=True, print_stats=True):
    # Print cube statistics
    if print_cube:
        print(cube)

    if print_stats:
        #        TODO -  Replace with logs
        print("####START####")
        print("cube has_lazy_data before stats::", cube.has_lazy_data())
        print("cube shape::{} cube ndim::{} cube data type::{} ".format(
            cube.shape, cube.ndim, type(cube.core_data())))
        print("cube standard_name::{} cube long_name:: {}:: cube var_name::{} cube name::{} ".format(
            cube.standard_name, cube.long_name, cube.var_name, cube.name()))
#         print("cube units: {} cube data.max ::{}  ".format(cube.units, cube.data.max()))
        print("cube units: {}  ".format(cube.units))
        print("cube attributes {}".format(cube.attributes))
        print("cube cell_methods {}".format(cube.cell_methods))
        print()
        print("cube shape {}".format(cube.shape))

        coord_list = cube.coords()
        len_coord = len(coord_list)
        print("coord len:: {}", format(len_coord))
        for i in range(len_coord):
            coord = coord_list[i]
            print("index::{} coord name:: {}".format(i, coord.name()))
#         coord_names = [coord.name() for coord in cube.coords()]
#         print("coord names::{}".format(coord_names))
        print("cube metadata ::{}".format(cube.metadata))
        print("cube has_lazy_data after stats::", cube.has_lazy_data())
#         print("cube data type ::{}".format(type(cube.core_data())))

        print("####END####")


def download_locally(bucket_name, key, tmp_file_name):    
    s3.download_file(bucket_name, key, tmp_file_name)

def download_files(conn, bucket, subfolder='', ):
    contents = conn.list_objects(
        Bucket=bucket, Prefix=subfolder, MaxKeys=10)['Contents']
    for f in contents:
        key = f['Key']
        tmp_file_name = os.path.join("/tmp/", key)
        print("download_files::tmp_file_name::{}".format(tmp_file_name))
        # download_locally (bucket, key, tmp_file_name)
        s3.download_file(bucket, key, tmp_file_name)

# upload file to S3
def upload_file(conn, file_name, bucket, upload_key):
    conn.upload_file(Filename=file_name, Bucket=bucket, Key=upload_key)


# Filter data by cube.ndim=3 and only 'projection_y_coordinate', 'projection_x_coordinate'
def filter_cube(cube):

    if cube.ndim == 2 and [coord.name() in ['projection_y_coordinate', 'projection_x_coordinate'] for coord in cube.coords()]:
        return True
    else:
        return False

# Compute latitudes and longitude values converting from projection coordinates to geographic


def compute_coordinates(dim_cords, x, y):
    latitude = None
    longitude = None
    #   Referred to https://jingwen-z.github.io/how-to-convert-projected-coordinates-to-latitude-longitude-by-python/
    if dim_cords[0].standard_name == "projection_y_coordinate" and dim_cords[1].standard_name == "projection_x_coordinate":

        transformer = pyproj.Transformer.from_crs("epsg:3857", "epsg:4326")
#         returns tuple of latttiude and longitude
        coordinates = transformer.transform(x, y)
#         print ("compute_coordinates::coordinates::{}".format(coordinates))
        latitude = coordinates[0]
        longitude = coordinates[1]
#         we should get the lattitudes and longitude values
    else:
        latitude = x
        longitude = y

    return latitude, longitude

# Process cube data and create a CSV file


def process_cube(cube_orig, unit, coord_zero_slice_len=0, coord_one_slice_len=0):
    cube = cube_orig
    if coord_zero_slice_len != 0 and coord_one_slice_len != 0:
        cube = cube_orig[0:coord_zero_slice_len, 0:coord_one_slice_len]

    # cube.convert_units(unit)
    dim_cords = cube.dim_coords
    dim_time = cube.coord('time')

    dim_dates = dim_time.units.num2date(dim_time.points)
#     print("dim_dates = {}".format(dim_dates))
#     conver time to readable date
    date_list = []
    np_time = ""
    for date_time in dim_dates:
        date_as_str = date_time.strftime("%Y-%m-%d %H:%M:%S")
        date_list.append(date_as_str)
    np_time = np.array(date_list)
#     print("List of Numpy time : {}".format(np_time))

#     x, y projections values
    coord_y = cube.coord(dim_cords[0].standard_name).points
    coord_x = cube.coord(dim_cords[1].standard_name).points
    
    # Create CSV header
    standard_name = cube.standard_name
    if standard_name is None:  
        return
    csv_header_list = []
    last_column_name = str(standard_name) + "-" + str(cube.units)    
    if(len(dim_cords) == 2):
        csv_header_list.append('time')
        csv_header_list.append('latitude')
        csv_header_list.append('longitude')    
    
    csv_header_list.append(last_column_name)
    # Write to CSV file
    
    csv_temp_folder = "/tmp/"
    csv_file_name = str(standard_name) + "_" + str(np_time[0]) + ".csv"
    csv_full_name = csv_temp_folder + csv_file_name
    with open(csv_full_name, 'w', newline='') as csvFile:
        csv_file_writer = csv.DictWriter(
            csvFile, delimiter=',', fieldnames=csv_header_list)
        csv_file_writer.writeheader()
        if(len(dim_cords) == 2):

            #             read individual data points
            for i in range(cube.shape[0]):
                for j in range(cube.shape[1]):

                    latitude, longitude = compute_coordinates(
                        dim_cords, coord_x[i], coord_y[i])
#                     skip the record
                    if latitude is None or longitude is None:
                        continue
                    row_dict = {}
                    row_dict['time'] = np_time[0]

                    row_dict['latitude'] = latitude
                    row_dict['longitude'] = longitude
                    row_dict[last_column_name] = cube.data[i][j]
#                     print (row_dict)
                    csv_file_writer.writerow(row_dict)

    upload_key = last_column_name + "/" + csv_file_name
#     upload the proccessed csv files to S3
    upload_file(s3, csv_full_name, csv_output_bucket, upload_key)

    os.remove(csv_full_name)

    return "SUCCESS"
