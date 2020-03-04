'''Visualizing-Business-Process-Evolution

author Anton Yeshchenko
'''

import os
from pm4py.objects.log.importer.xes import factory as xes_import_factory
from pathlib import Path
from pm4py.objects.log.util import sorting
import csv


NUMBER_OF_PARTS = 4


# file = Path(r'C:\Users\anton\ownCloud\Documents-Anton\Data-C\Event-logs\Sepsis')
# file = file / "sepsis_timestamp_sorted.xes"

file = Path(r'C:\Users\anton\ownCloud\Documents-Anton\Data-C\Event-logs')
#folder = "Traffic-fines"
folder = "Italian-help-desk"
folder = "bpic2011-Hospital-log"
folder = "bpic2015"
#file = file / folder /  "italian_help_desk_timestamp_sorted.xes"
#file = file / folder /  "Hospital_log.xes"
file = file / folder /  "BPIC15_1.xes"


def ensure_path_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)
ensure_path_exists(file)

################################
################################
# ADDITIONAL FUNCTIONS FIRST
def intersection_date(t1start,t1end,t2start,t2end):
    return (t1start <= t2start <= t1end) or (t2start <= t1start <= t2end)
################################
################################



# IMPORT FILE IN XES
parameters = {"timestamp_sort": True}
log = xes_import_factory.apply(str(file), parameters=parameters)


date1 = log[0]._list[-1]._dict["time:timestamp"]
date2 = log[-1]._list[-1]._dict["time:timestamp"]

# SORT FILES IN THE ASCENDING ORDER OF THE CYCLE TIME
sorted_log = sorting.sort_lambda(log,
        lambda x: (x._list[-1]._dict["time:timestamp"] - x._list[0]._dict["time:timestamp"]) , reverse=False)
         # lambda x: x.attributes["concept:name"], reverse=False)


# SPLITS LOG INTO N PARTS (SORTED RIGHT BY FIRST TIMESTAMP)
def split_into_n_parts(log, n):
    cl = []
    lc_bound = [0]
    for i in range(1,n+1):
        lc_bound.append(int(len(log._list) / n) * i)

    for i in range(1,n+1):
        cl_temp = log._list[lc_bound[i-1]:lc_bound[i]]
        cl_temp_sorted_tm = sorted(cl_temp, key=lambda x: x._list[0]._dict['time:timestamp'], reverse=True)
        cl.append(cl_temp_sorted_tm)
    return cl


clusters = split_into_n_parts(sorted_log,NUMBER_OF_PARTS)

# WITHIN ONE PART
# SPLOT INTO N (50) PARTS, AND COUNT CHARACTERISTIC

# here is clustering per time range
def count_per_timerange(data, date1,date2,n = 50):
    date_diff = date2 - date1
    date_diff_st = date_diff / n

    calculate_characteristic = [0] * (n+1)

    timeranges = [date1]
    for i in range(n):
        timeranges.append(date1 + date_diff_st * i)

    for i in range(1,len(timeranges)):
        for j in data:
            if intersection_date(j._list[0]._dict['time:timestamp'],j._list[-1]._dict['time:timestamp'],timeranges[i-1],timeranges[i]):
                calculate_characteristic[i] += 1

    return calculate_characteristic

# here is clustering per cycle time
def count_per_timerange_cycle_time(data, date1,date2,n = 50):
    date_diff = date2 - date1
    date_diff_st = date_diff / n

    calculate_characteristic = [0] * (n+1)

    timeranges = [date1]
    for i in range(n):
        timeranges.append(date1 + date_diff_st * i)

    for i in range(1,len(timeranges)):
        char_ind = 0
        for j in data:
            if intersection_date(j._list[0]._dict['time:timestamp'],j._list[-1]._dict['time:timestamp'],timeranges[i-1],timeranges[i]):
                char_ind += 1
                calculate_characteristic[i] += (j._list[-1]._dict['time:timestamp'] - j._list[0]._dict['time:timestamp']).total_seconds()/3600
        if not abs (calculate_characteristic[i]) < 0.001:
            calculate_characteristic[i] /= char_ind # calculate average
    return calculate_characteristic


ou = []
# for i in range(NUMBER_OF_PARTS):
#     ou.append(count_per_timerange(clusters[i],date1,date2))

for i in range(NUMBER_OF_PARTS):
    ou.append(count_per_timerange_cycle_time(clusters[i],date1,date2))


ou = list(map(list, zip(*ou)))


# OUTPUT VALUE
with open("out_cycle.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerows(ou)

print ("done")


