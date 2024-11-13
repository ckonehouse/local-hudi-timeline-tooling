import os
import re
from pathlib import Path
import sys
import json
from tabulate import tabulate
from datetime import datetime

# Set the path to the timelines directory

COW = "COPY_ON_WRITE"
MOR = "MERGE_ON_READ"

table_totals = {
    "num_tables":0,
    "bytes_day":0,
    "total_update_perc":0,
    "total_jobs_day": 0
}
def calculate_aggregate_metrics(tables):
    
    aggregate_metrics = {
        COW:{
            "mutable":table_totals.copy(),
            "append_only":table_totals.copy()            
        },
        MOR:{
            "mutable": table_totals.copy(),
            "append_only": table_totals.copy()
        }
    }
    
    for key, value in tables.items():
        table_stats = value.get("table_stats")
        if value["table_type"] == COW and table_stats.get("updatePercentage") == 0: #COW append
            
            aggregate_metrics[COW]['append_only']['num_tables'] += 1
            aggregate_metrics[COW]['append_only']['bytes_day'] += (table_stats['bytesWritten']/table_stats['numDays'])
            aggregate_metrics[COW]['append_only']['total_jobs_day'] += table_stats['numJobs']
            
        elif (value["table_type"] == COW) and (table_stats.get("updatePercentage") != 0.0): #COW mutable
    
            aggregate_metrics[COW]['mutable']['num_tables'] += 1
            aggregate_metrics[COW]['mutable']['bytes_day'] += (table_stats['bytesWritten']/table_stats['numDays'])
            aggregate_metrics[COW]['mutable']['total_jobs_day'] += table_stats['numJobs']
            aggregate_metrics[COW]['mutable']['total_update_perc'] += table_stats['updatePercentage']
            
        elif (value['table_type'] == MOR) and (table_stats.get("updatePercentage") == '0'): #MOR append
            
            aggregate_metrics[MOR]['append_only']['num_tables'] += 1
            aggregate_metrics[MOR]['append_only']['bytes_day'] += (table_stats['bytesWritten']/table_stats['numDays'])
            aggregate_metrics[MOR]['append_only']['total_jobs_day'] += table_stats['numJobs']
            
        elif (value['table_type'] == MOR) and (table_stats.get("updatePercentage") != '0'): #MOR mutable
            
            aggregate_metrics[MOR]['mutable']['num_tables'] += 1
            aggregate_metrics[MOR]['mutable']['bytes_day'] += (table_stats['bytesWritten']/table_stats['numDays'])
            aggregate_metrics[MOR]['mutable']['total_jobs_day'] += table_stats['numJobs']
            aggregate_metrics[MOR]['mutable']['total_update_perc'] += table_stats['updatePercentage']#MOR mutable    
    return aggregate_metrics


def convert_bytes(size_in_bytes):
    # Define units
    units = ["Bytes", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"]
    # Set initial index for units
    index = 0

    # Convert the bytes into the appropriate unit
    while size_in_bytes >= 1024 and index < len(units) - 1:
        size_in_bytes /= 1024
        index += 1

    # Format result to 2 decimal places
    return f"{size_in_bytes:.2f} {units[index]}"


def get_commit_frequency(recent_commits, numDays):
    commitsPerDay = len(recent_commits)/numDays
    return commitsPerDay

def list_days_of_commits(commits):
    print(commits)

timelines_path = Path("timelines")

# Check if the timelines directory exists
if not timelines_path.is_dir():
    print(f"Error: {timelines_path} directory does not exist.")
    sys.exit(1)

# Initialize a list to store timeline directories
timeline_directories = [item.name for item in timelines_path.iterdir() if item.is_dir()]

# Check if use_lakeview is set to "n" (example, adjust as needed)
use_lakeview = "n"

tables = {}

if use_lakeview == "n":
    for dir_name in timeline_directories:
        hoodie_dir = timelines_path / dir_name / ".hoodie"
        print(f"Looking at .commit files in {hoodie_dir}")

        # Check if the .hoodie directory exists within each timeline directory
        if hoodie_dir.is_dir():

            # get table type
            properties_file = hoodie_dir / "hoodie.properties"
            table_type = None
            if properties_file.is_file():
                with open(properties_file, "r") as file:
                    for line in file:
                        if line.startswith("hoodie.table.type"):
                            table_type = line.split("=")[-1].strip()
                            break
                if table_type:
                    print(f"hoodie.tabletype for {dir_name}: {table_type}")
                else:
                    print(f"hoodie.tabletype not found in {properties_file}")
            else:
                print(f"No hoodie.properties file found in {hoodie_dir}")
            
            tables[dir_name] = {
                "table_type":table_type
            }

            #COW tables
            table_stats = {
                "bytesWritten":0,
                "numDays":0,
                "numWrites":0,
                "numInserts":0,
                "numUpdates":0,
                "numDeletes":0,
                "updatePercentage": 0,
                "servicesEnabled": []
            }

            #Check if clustering/cleaning enabled
            has_clustering = any(file.endswith('.replacecommit') for file in os.listdir(hoodie_dir))
            has_cleaning = any(file.endswith('.clean') for file in os.listdir(hoodie_dir))
            if has_clustering:
                table_stats["servicesEnabled"].append("clustering")
            if has_cleaning:
                table_stats["servicesEnabled"].append("cleaning")



            if table_type == COW:
                # Find .commit files directly in the .hoodie directory 
                commit_files = [
                    f for f in hoodie_dir.iterdir() 
                    if f.is_file() and f.suffix == '.commit' and re.match(r"\d{8}\d*\.commit", f.name)
                ]


                sorted_commit_files = sorted(
                    commit_files,
                    key=lambda x: x.stem[:8],  # Sort based on YYYYMMDD
                    reverse=True
                )

# Select files from the 7 most recent unique dates
                recent_commit_files = []
                seen_dates = set()

                for file in sorted_commit_files:
                    file_date = datetime.strptime(file.stem[:8], "%Y%m%d").date()
                    if file_date not in seen_dates:
                        seen_dates.add(file_date)
                    if len(seen_dates) == 7:  # Stop once we have 7 unique dates
                        break
                    recent_commit_files.append(file)
                
                table_stats["numDays"] = len(seen_dates)
                
                if recent_commit_files:
                    print(f"Recent .commit files in {hoodie_dir}:")
                    
                    commit_frequency = get_commit_frequency(recent_commit_files, table_stats["numDays"])
                    table_stats["numJobs"] = commit_frequency
                    table_stats["bytesWritten"] = 0

                    for commit_file in recent_commit_files:
                        with open(commit_file,'r') as commit:
                            commit_data = json.load(commit)
                            for key,value in commit_data["partitionToWriteStats"].items():
                                for entry in value:
                                    table_stats["bytesWritten"] += entry["totalWriteBytes"]
                                    table_stats["numInserts"] += entry["numInserts"]
                                    table_stats["numDeletes"] += entry["numDeletes"]
                                    table_stats["numUpdates"] += entry["numUpdateWrites"]
                                    table_stats["numWrites"] += entry["numWrites"]
                                    table_stats["updatePercentage"] = (table_stats["numUpdates"] + table_stats["numDeletes"])/table_stats["numWrites"]
                    
                    tables[dir_name]["table_stats"]= table_stats
                    
                else:
                    print(f"No .commit files found in {hoodie_dir}")
            
            elif table_type==MOR:
                deltacommit_files = [
                    f for f in hoodie_dir.iterdir() 
                    if f.is_file() and f.suffix == '.deltacommit' and re.match(r"\d{8}\d*\.deltacommit", f.name)
                ]

                sorted_deltacommit_files = sorted(
                    deltacommit_files,
                    key=lambda x: x.stem[:8],  # Sort based on YYYYMMDD
                    reverse=True
                )

# Select files from the 7 most recent unique dates
                recent_deltacommit_files = []
                seen_dates = set()
                
                for file in sorted_deltacommit_files:
                    file_date = datetime.strptime(file.stem[:8], "%Y%m%d").date()
                    if file_date not in seen_dates:
                        seen_dates.add(file_date)
                    if len(seen_dates) == 7:# Stop once we have 7 unique dates
                        break
                    recent_deltacommit_files.append(file)
                table_stats["numDays"] = len(seen_dates)
                
                if sorted_deltacommit_files:
                    commit_frequency = get_commit_frequency(sorted_deltacommit_files, table_stats["numDays"])
                    table_stats["numJobs"] = commit_frequency

                    for commit_file in sorted_deltacommit_files:
                        with open(commit_file,'r') as commit:
                            commit_data = json.load(commit)
                            for key,value in commit_data["partitionToWriteStats"].items():
                                for entry in value:
                                    table_stats["bytesWritten"] += entry["totalWriteBytes"]
                                    table_stats["numInserts"] += entry["numInserts"]
                                    table_stats["numDeletes"] += entry["numDeletes"]
                                    table_stats["numUpdates"] += entry["numUpdateWrites"]
                                    table_stats["numWrites"] += entry["numInserts"] + entry["numUpdateWrites"]
                                    table_stats["updatePercentage"] = (table_stats["numUpdates"] + table_stats["numDeletes"])/(table_stats["numWrites"]+table_stats["numDeletes"])
                    
                    tables[dir_name]["table_stats"]= table_stats

                # Check if compaction is running
                has_compaction = (any(file.endswith('compaction.inflight') for file in os.listdir(hoodie_dir))) & any(file.endswith('compaction.requested') for file in os.listdir(hoodie_dir)) & any(file.endswith('.commit') for file in os.listdir(hoodie_dir))
                if has_compaction:
                    table_stats["servicesEnabled"].append("compaction")

            else:
                print(f"Directory {hoodie_dir} does not exist")

aggregates = calculate_aggregate_metrics(tables)

table_data = []
for key, value in tables.items():
    row = {
        'Table': key,
        'Table Type': value['table_type'],
        'Bytes Written Total': convert_bytes(value.get('table_stats', {}).get('bytesWritten', 'N/A')),
        'Num Days': value.get('table_stats', {}).get('numDays', 'N/A'),
        'Bytes Written / Day ': convert_bytes(value.get('table_stats',{}).get('bytesWritten') / value.get('table_stats',{}).get('numDays')),
        'Num Ingestion Jobs': value.get('table_stats',{}).get('numJobs','N/A'),
        'Num Writes': value.get('table_stats', {}).get('numWrites', 'N/A'),
        'Num Inserts': value.get('table_stats', {}).get('numInserts', 'N/A'),
        'Num Updates': value.get('table_stats', {}).get('numUpdates', 'N/A'),
        'Num Deletes': value.get('table_stats', {}).get('numDeletes', 'N/A'),
        'Update Percentage': value.get('table_stats', {}).get('updatePercentage', 'N/A'),
        'Enabled Services':value.get('table_stats', {}).get('servicesEnabled', 'N/A'),
    }
    table_data.append(row)

totals = []
for table_type, value in aggregates.items():
    for workload in value.keys():
        row = {}
        row["Table Type"] = table_type
        row["Workload"] = workload
        row["Bytes Written / Day "] = convert_bytes(value[workload]['bytes_day'])
        row["Avg Update Percentage"] = value[workload]['total_update_perc']/value[workload]['num_tables'] if value[workload]['num_tables']!=0 else value[workload]['total_update_perc']
        row["Avg Num Ingestion Jobs / Day"] = value[workload]['total_jobs_day']/value[workload]['num_tables'] if value[workload]['num_tables']!=0 else value[workload]['total_update_perc']
        totals.append(row)
    
aggregate_table = tabulate(totals, headers = "keys",tablefmt="grid")
granular_table = tabulate(table_data, headers = "keys",tablefmt="grid")

with open("analysis_table.txt","w") as file:
    file.write("Aggregate Data on Hudi Workloads\n")
    file.write(aggregate_table)
    file.write("\nTable by Table Breakdown\n")
    file.write(granular_table)