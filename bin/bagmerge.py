#!/usr/bin/env python
import sys
import roslib;
from typing import Any, Union

roslib.load_manifest('bagedit')
import rospy
import rosbag
from rospy import rostime
import argparse
import os

def parse_args():
    parser = argparse.ArgumentParser(
        prog = 'bagmerge.py',
        description='Merges two bagfiles.')
    parser.add_argument('-o', type=str, help='name of the output file', 
        default = None, metavar = "output_file")
    parser.add_argument('-t', type=str, help='topics which should be merged to the main bag', 
        default = None, metavar = "topics")
    parser.add_argument('-i', help='reindex bagfile', 
        default = False, action="store_true")
    parser.add_argument('main_bagfile', type=str, help='path to a bagfile, which will be the main bagfile')
    parser.add_argument('bagfiles', nargs='+', type=str, help='path to the bagfile(s) which should be merged to the main bagfile')
    args = parser.parse_args()
    return args

def get_next(bag_iter, reindex = False, 
        main_start_time = None, start_time = None, 
        topics = None):
    try:
        result = bag_iter.next()
        if topics != None:
            while not result[0] in topics:
                result = bag_iter.next()
        if reindex:
            return (result[0], result[1], 
                result[2] - start_time + main_start_time)
        return result
    except StopIteration:
        return None

def merge_bag(main_bagfile, bagfiles, outfile = None, topics = None,
        reindex = True):
    #get min and max time in bagfile
    main_limits = get_limits(main_bagfile)
    limits = [get_limits(bagfile) for bagfile in bagfiles]
    #check output file
    if outfile == None:
        pattern = main_bagfile + "_merged_%i.bag"
        outfile = main_bagfile + "_merged.bag"
        index = 0
        while (os.path.exists(outfile)):
            outfile = pattern%index
            index += 1
    #output some information
    print "merge bag %s in %s"%(bagfile, main_bagfile)
    print "topics filter: ", topics
    print "writing to %s."%outfile
    #merge bagfile
    outbag = rosbag.Bag(outfile, 'w')
    main_bag = rosbag.Bag(main_bagfile).__iter__()
    bags = [rosbag.Bag(bagfile).__iter__() for bagfile in bagfiles]
    next = [get_next(main_bag)] + [get_next(bags[i], reindex, main_limits[0], limits[i][0], topics) for i in range(len(bagfiles))]
    def find_next(next):
        i = 0
        next_time = next[0][2].to_sec() if next[0] is not None else float('inf')
        for n in range(1, len(next)):
            if next[n] is None:
                continue
            test_time = next[n][2].to_sec()
            if test_time < next_time:
                i = n
                next_time = test_time
        return i, next_time
    def update_next(next, i):
        if i == 0:
            next[0] = get_next(main_bag)
        else:
            print(len(next), len(limits), i)
            next[i] = get_next(bags[i-1], reindex, main_limits[0], limits[i-1][0], topics)
    try:
        next_i, next_time = find_next(next)  # type: (int, Union[float, rostime.Time])
        # print(type(next_time))
        while next_time < float('inf'):
            outbag.write(next[next_i][0], next[next_i][1], next[next_i][2])
            update_next(next, next_i)
            next_i, next_time = find_next(next)
    finally:
        outbag.close()

def get_limits(bagfile):
    print "Determine start and end index of %s..."%bagfile
    end_time = None
    start_time = None

    for topic, msg, t in rosbag.Bag(bagfile).read_messages():
        if start_time == None or t < start_time:
            start_time = t
        if end_time == None or t > end_time:
            end_time = t
    return (start_time, end_time)
    
if __name__ == "__main__":
    args = parse_args()
    print args
    if args.t != None:
        args.t = args.t.split(',')
    merge_bag(args.main_bagfile, 
        args.bagfiles,
        outfile = args.o,
        topics = args.t,
        reindex = args.i)
