#! /usr/bin/python3
# -*- coding: utf-8 -*-

# data_export_script_v3.4.1

import gzip, sqlite3, os, csv, logging, datetime, sys
from datetime import datetime
from subprocess import call

print("\n++++++++++ IMU Event Log Script - v3.4.1u ++++++++++\n")

# calculate accel thresholds in m/s/s for SQL query -- Change X in str(X) below to whole of decimal acceleration number. 
param_x = str(11.7)
param_y = str(11.7)

print("")

def open_gzip_sqlite_db(db_path): # create temporary db file and populate with currently loaded telemetry file data
    with gzip.open(db_path, 'rb') as f_in:
        with open('temp.db', 'wb') as f_out:
            f_out.write(f_in.read())

    return sqlite3.connect('temp.db')

def process_database(db_path,output_file,param_x,param_y): # process the selected database file
    print("INFO: Looking for new database file in directory")
    conn = open_gzip_sqlite_db(db_path) # open file
    cur = conn.cursor()
    print("INFO: Loading database file", db_path)

    # check if tables exist in log
    print("INFO: Checking if correct tables exist in selected database file")
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='machine_udp_imu_hs_feedback'")
    imu_exists = cur.fetchone()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='beacon'")
    beacon_exists = cur.fetchone()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='strat'")
    strat_exists = cur.fetchone()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='speed'")
    speed_exists = cur.fetchone()

    if not imu_exists or not beacon_exists or not strat_exists: # if tables do or don't exist in the log file
        missing_tables = []
        if not imu_exists:
            missing_tables.append('machine_udp_imu_hs_feedback')
            print("WARNING: machine_udp_imu_hs_feedback table does not exist in selected database file")
        if not beacon_exists:
            missing_tables.append('beacon')
            print("WARNING: beacon table does not exist in selected database file")
        if not strat_exists:
            missing_tables.append('strat')
            print("WARNING: strat table does not exist in selected database file")
        if not speed_exists:
            missing_tables.append('speed')
            print("WARNING: speed table does not exist in selected database file")
        print("INFO: Ignoring file; proceeding to next file in the folder")
        logging.error("File {} couldn't be processed. Missing Tables: {}".format(db_path, ', '.join(missing_tables))) # adds line to log file explaining why file was ignored
        conn.close() # close the db connection
        os.remove('temp.db') # delete temp database file
        return
    print("INFO: All required tables exist in database file.")

    print("INFO: Filtering data in database file. This may take several minutes...")
    # sql query to execute
    sql_query = """
    SELECT datetime(m.recv_time, 'unixepoch','0 hour'), s.x, s.y, s.incursion, s.area_id, m.NEEF_x_Accel, m.NEEF_y_Accel, m.EEF_x_Accel, m.EEF_y_Accel, p.speed_ms, b.machine_id, b.sponsor_id, b.control, b.intention, b.codes
    FROM machine_udp_imu_hs_feedback m
    LEFT OUTER JOIN beacon b
    ON round(m.recv_time) = round(b.recv_time)
    LEFT OUTER JOIN strat s
    ON round(m.recv_time) = round(s.recv_time)
    LEFT OUTER JOIN speed p
    ON round(m.recv_time) = round(p.recv_time)
    WHERE (m.EEF_x_Accel + m.EEF_y_Accel + m.NEEF_x_Accel + m.NEEF_y_Accel) > -80000 AND
        (m.NEEF_x_Accel > {x}
        OR m.NEEF_y_Accel > {y}
        OR m.EEF_x_Accel > {x}
        OR m.EEF_y_Accel > {y}
        OR (m.NEEF_x_Accel < -{x} AND m.NEEF_x_Accel != -32738)
        OR (m.NEEF_y_Accel < -{y} AND m.NEEF_y_Accel != -32738)
        OR (m.EEF_x_Accel < -{x} AND m.EEF_x_Accel != -32738)
        OR (m.EEF_y_Accel < -{y} AND m.EEF_y_Accel != -32738)
        )
    GROUP BY datetime(m.recv_time, 'unixepoch','0 hour')
    """

    threshold_dictionary = {
        "x": param_x,
        "y": param_y,
    }

    cur.execute(sql_query.format(**threshold_dictionary)) # execute the sql query
    results = cur.fetchall() # get results from query
    print("INFO: Number of events found in file:", len(results))
    print("INFO: Writing filtered IMU event data to CSV output file")
    if os.path.exists(output_file): # check if CSV output file already exists
        file_counter = 1
    else:
        file_counter = 0
    
    with open(output_file, 'a', newline='') as f_handle: # opens the CSV output file to append data
        writer = csv.writer(f_handle)
        if file_counter == 0: # if output file doesn't already exist
            header = ['time_utc','loc_x','loc_y','prox_warn','area_id','fimu_x','fimu_y','rimu_x','rimu_y','speed_ms','machine_id','ros_id','control','state','mas_codes']
            writer.writerow(header)
        for row in results: # for each row of data from the query results, writes the data to the output file
            writer.writerow(row)

    conn.close() # disconnect from db file
    print("INFO: Cleaning up temporary database file")
    os.remove('temp.db') # delete temp database

def main():
    # Get current time first
    now = datetime.now()
    cur_datetime_abrv = now.strftime("%Y-%m-%d_%H%M%S")

    # Original Linux path (comment this when running on Windows)
    #db_directory = '/data/sqlogger/'
    #output_file = '/data/common/IMU_event_log_'+cur_datetime_abrv+'.csv'
    
    # Windows path (uncomment this when running on Windows)
    db_directory = './data/sqlogger/'
    output_file = './data/common/IMU_event_log_'+cur_datetime_abrv+'.csv'
    
    # Original Linux path (comment these when running on Windows)
    #VehicleList_Path = '/data/common/vehicle_list.csv'
    #AreaList_Path = '/data/common/area_list.csv'
    #ROSList_Path = '/data/common/ros_list.csv'

    # Windows path (uncomment these when running on Windows)
    VehicleList_Path = './data/common/vehicle_list.csv'
    AreaList_Path = './data/common/area_list.csv'
    ROSList_Path = './data/common/ros_list.csv'

    file_count = 0 # initialize file count
    for filename in os.listdir(db_directory): # for each file in the folder
        date_str = filename.split('_')[1] # get date from file name
        file_date = datetime.strptime(date_str, '%Y-%m-%d') # assign date to file based on name
        if filename.endswith('.db.gz') and filename.startswith('KT400212_2025-02-23_063709'): # for each .db.gz file in the folder
            file_count = file_count + 1 # increase file count
    if file_count == 0: # if no files found
        print("INFO: No telemetry files found for the selected date range.\n")
        sys.exit() # exit script
    else: # if files are found
        print("INFO: Telemetry files found for the selected date range:", file_count)
        print("")

    # configure error log file
    now = datetime.now()
    cur_datetime_abrv = now.strftime("%Y-%m-%d_%H%M%S")
    log_file_name = 'imu_report_error_'+cur_datetime_abrv+'.log'
    logging.basicConfig(filename=log_file_name, level=logging.ERROR, format='%(asctime)s - %(message)s')

    for filename in os.listdir(db_directory): # for each file in the folder
        date_str = filename.split('_')[1] # get date from file name
        file_date = datetime.strptime(date_str, '%Y-%m-%d') # assign date to file based on name
        if filename.endswith('.db.gz') and filename.startswith('KT400212_2025-02-23_063709'): # for each .db.gz file in the folder
            db_path = os.path.join(db_directory, filename) # set db path based on file name
            process_database(db_path, output_file,param_x,param_y) # process the file

    print("\nINFO: No further files in the folder to review\n")

    print("INFO: Reading configuration from existing CSV files")
    
    # Original code - commented out
    #with open(AreaList_Path, 'r') as file: # build dictionary for area_id for map name retrieval
    #    reader = csv.reader(file)
    #    next(reader, None)
    #    usable_rows = list(reader)
    #    area_name_list = {row[0]: row[1].strip() for row in usable_rows[:-1]}

    # New code for reading area list
    with open(AreaList_Path, 'r') as file:
        reader = csv.reader(file)
        next(reader, None)  # Skip header
        area_name_list = {}
        for row in reader:
            if len(row) >= 2 and not row[0].startswith('('):  # Skip footer row
                area_name_list[row[0]] = row[1].strip()

    # Original code - commented out
    #with open(ROSList_Path, 'r') as file: # build dictionary for sponsor_id for ROS name retrieval
    #    reader = csv.reader(file)
    #    next(reader, None)
    #    usable_rows = list(reader)
    #    ros_name_list = {row[0]: row[1].strip() for row in usable_rows[:-1]}

    # New code for reading ROS list
    with open(ROSList_Path, 'r') as file:
        reader = csv.reader(file)
        next(reader, None)  # Skip header
        ros_name_list = {}
        for row in reader:
            if len(row) >= 2 and not row[0].startswith('('):  # Skip footer row
                ros_name_list[row[0]] = row[1].strip()

    # Original code - commented out
    #with open(VehicleList_Path, 'r') as file: # build dictionary for vehicle_id for machine name retrieval
    #    reader = csv.reader(file)
    #    next(reader, None)
    #    usable_rows = list(reader)
    #    vehicle_name_list = {row[0]: row[1].strip() for row in usable_rows[:-1]}

    # New code for reading vehicle list - with debug print
    with open(VehicleList_Path, 'r') as file:
        reader = csv.reader(file)
        next(reader, None)  # Skip header
        vehicle_name_list = {}
        for row in reader:
            if len(row) >= 2 and not row[0].startswith('('):  # Skip footer row
                vehicle_name_list[row[0]] = row[1].strip()
        print("DEBUG: Loaded vehicle IDs:", vehicle_name_list)  # Debug print

    # build array of values for 'control' and 'state' types
    control_list = {'0': 'Teleremote', '1': 'Copilot', '2': 'Autopilot', '3': 'Recovery'}
    state_list = {'0': 'Load', '1': 'Haul', '2': 'Dump'}

    # read existing CSV output file and update values - with debug print
    print("INFO: Decoding telemetry values in CSV output file")
    with open(output_file, 'r') as file:
        reader = csv.reader(file)
        next(reader, None)
        updated_csv = []
        for row in reader:
            map_id = row[4]
            vehicle_id = row[10]
            sponsor_id = row[11]
            control_num = row[12]
            state_num = row[13]
            
            print(f"DEBUG: Looking up vehicle_id: {vehicle_id}")  # Debug print
            
            # Lookup values in dictionaries
            area_id = area_name_list.get(map_id, 'Unknown')
            machine_id = vehicle_name_list.get(vehicle_id, 'Unknown')
            ros_id = ros_name_list.get(sponsor_id, 'Unknown')
            control = control_list.get(control_num, 'Unknown')
            state = state_list.get(state_num, 'Unknown')
            
            print(f"DEBUG: Found machine_id: {machine_id}")  # Debug print
            
            # Update row with decoded values
            row[4] = area_id
            row[10] = machine_id
            row[11] = ros_id
            row[12] = control
            row[13] = state
            updated_csv.append(row)

    # Write updated data with header
    with open(output_file, 'w', newline='') as file:
        writer = csv.writer(file)
        header = ['time_utc','loc_x','loc_y','prox_warn','area','fimu_x','fimu_y','rimu_x','rimu_y','speed_ms','machine','ros_id','control','state','mas_codes']
        writer.writerow(header)
        writer.writerows(updated_csv)

    # delete temporary csv reference files
    # print("INFO: Cleaning up...")
    # os.remove(VehicleList_Path)
    # os.remove(AreaList_Path)
    # os.remove(ROSList_Path)

if __name__ == "__main__":
    main()

print("\nProcess completed successfully!\n")


