#!/usr/bin/env python
import rosbag
from rospy import rostime, Time
from tqdm import tqdm
from bagmerge import get_limits, get_next
import argparse
import os


def parse_args():
    parser = argparse.ArgumentParser(
        prog='bagmerge.py',
        description='Merges two bagfiles.')
    parser.add_argument('-o', type=str, help='name of the output file -- should include two "%i"s',
                        default=None, metavar="output_file")
    parser.add_argument('-t', type=str, help='topics which should be included',
                        default=None, metavar="topics")
    parser.add_argument('-i', help='reindex bagfile',
                        default=False, action="store_true")
    parser.add_argument('bagfile', type=str, help='path to a bagfile, which will be the main bagfile')
    parser.add_argument('num_split', type=int,
                        help='number of equal duration bagfiles')
    parser.add_argument('--start', type=float, help='split from start time', default=float('-inf'), metavar="start")
    parser.add_argument('--end', type=float, help='split to end time', default=float('inf'), metavar="end")
    parser.add_argument('--bz2', help='bz2 compression', default=False, action="store_true")
    args = parser.parse_args()
    return args


def split_bag(bagfile, splits, outfile_pattern=None, topics=None, compression=rosbag.Compression.NONE,
              reindex=True, start_limit=float('-inf'), end_limit=float('inf'), debug=False):
    # get min and max time in bagfile
    if start_limit == float('-inf') or end_limit == float('inf'):
        limits = get_limits(bagfile)
        start_limit = limits[0] if start_limit == float('-inf') else start_limit
        end_limit = limits[1] if end_limit == float('inf') else end_limit
    assert end_limit > start_limit
    # check output file
    if outfile_pattern is None:
        outfile_pattern = bagfile.rstrip('.bag') + "_part_%i_of_%i.bag"
    else:
        assert outfile_pattern.count("%i") == 2
    # output some information
    print("topics filter: ", topics)
    # split bagfile
    inbag = rosbag.Bag(bagfile, 'r', skip_index=False, chunk_threshold=8 * 1024 * 1024)
    next_begin = start_limit
    first = True
    last_t = start_limit
    for i in range(splits):
        next_end = start_limit + (end_limit - start_limit) / splits * (i+1)
        out_filename = outfile_pattern % (i+1, splits)
        if debug:
            print("writing to %s." % out_filename)
        outbag = rosbag.Bag(out_filename, 'w', compression=compression, chunk_threshold=32 * 1024 * 1024)
        progress = tqdm(total=next_end - next_begin, unit='bag-s', unit_scale=True)
        try:
            for next in inbag.read_messages(topics, Time.from_sec(next_begin), Time.from_sec(next_end), None, True, False):
                # if next is None or next[2] > next_end:
                #     break
                # assert len(next) == 3
                # while next[2] < next_begin:
                #     assert first
                #     next = get_next(inbag, False, None, None, topics)
                # first = False
                if reindex:
                    next.timestamp = Time.from_sec(next[2].to_sec() - next_begin + start_limit)
                outbag.write(*next, raw=True)
                progress.update(next[2].to_sec() - last_t)
                last_t = next[2].to_sec()
        finally:
            outbag.close()
        next_begin = next_end


if __name__ == "__main__":
    args = parse_args()
    # print args
    if args.t != None:
        args.t = args.t.split(',')
    split_bag(args.bagfile,
              args.num_split,
              outfile_pattern=args.o,
              topics=args.t,
              compression=(rosbag.Compression.BZ2 if args.bz2 else rosbag.Compression.NONE),
              reindex=args.i,
              start_limit=args.start,
              end_limit=args.end)
