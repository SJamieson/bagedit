#!/usr/bin/env python
import rosbag
from rosbag.bag import BagMessage
from rospy import Time
from tqdm import tqdm
from bagmerge import get_limits, merge_bag
from bagsplit import split_bag
import argparse
import os


def parse_args():
    parser = argparse.ArgumentParser(
        prog='bagreverse.py',
        description='Reverses a bagfile.')
    parser.add_argument('-o', type=str, help='name of the output file',
                        default=None, metavar="output_file")
    parser.add_argument('-t', type=str, help='topics which should be included',
                        default=None, metavar="topics")
    parser.add_argument('bagfile', type=str, help='path to a bagfile, which will be the main bagfile')
    parser.add_argument('--start', type=float, help='reverse from start time', default=float('-inf'), metavar="start")
    parser.add_argument('--end', type=float, help='reverse to end time', default=float('inf'), metavar="end")
    parser.add_argument('--splits', type=int, help='for large files, use splits', default=1, metavar="splits")
    parser.add_argument('--bz2', help='bz2 compression', default=False, action="store_true")
    args = parser.parse_args()
    return args


def reverse_bag(bagfile, out_filename=None, topics=None, compression=rosbag.Compression.NONE,
                start_limit=float('-inf'), end_limit=float('inf'), debug=False):
    # get min and max time in bagfile
    if start_limit == float('-inf') or end_limit == float('inf'):
        limits = get_limits(bagfile)
        start_limit = limits[0] if start_limit == float('-inf') else start_limit
        end_limit = limits[1] if end_limit == float('inf') else end_limit
    assert end_limit > start_limit
    # check output file
    if out_filename is None:
        out_filename = bagfile.rstrip('.bag') + "_reversed.bag"
    # output some information
    print("topics filter: ", topics)
    # split bagfile
    inbag = rosbag.Bag(bagfile, 'r', skip_index=False, chunk_threshold=8 * 1024 * 1024)
    if debug:
        print("writing to %s." % out_filename)
    outbag = rosbag.Bag(out_filename, 'w', compression=compression, chunk_threshold=32 * 1024 * 1024)
    progress = tqdm(total=end_limit - start_limit, unit='bag-s', unit_scale=True, desc="Forward pass")
    def reversed_time(t):
        return Time.from_sec(end_limit - t.to_sec() + start_limit)
    try:
        messages = []
        last_t = start_limit
        for next in inbag.read_messages(topics, Time.from_sec(start_limit), Time.from_sec(end_limit), None, False, False):
            messages.append(next)
            progress.update(next[2].to_sec() - last_t)
            last_t = next[2].to_sec()
        last_t = end_limit
        progress = tqdm(total=end_limit - start_limit, unit='bag-s', unit_scale=True, desc="Backward pass")
        while len(messages) > 0:
            next = messages.pop()
            progress.update(last_t - next[2].to_sec())
            new_msg = next[1]
            if hasattr(new_msg, 'header'):
                new_msg.header.stamp = reversed_time(new_msg.header.stamp)
            elif hasattr(new_msg, 'transforms'):
                for i in range(len(new_msg.transforms)):
                    assert hasattr(new_msg.transforms[i], 'header')
                    new_msg.transforms[i].header.stamp = reversed_time(new_msg.transforms[i].header.stamp)
            else:
                print("Unrecognized message type: " + new_msg)
            outbag.write(next[0], new_msg, reversed_time(next[2]), raw=False)
            last_t = next[2].to_sec()
    finally:
        outbag.close()


if __name__ == "__main__":
    args = parse_args()
    # print args
    if args.t is not None:
        args.t = args.t.split(',')
    if args.o is None:
        args.o = args.bagfile.rstrip('.bag') + "_reversed.bag"
    if args.splits > 1:
        split_bag(args.bagfile, args.splits, outfile_pattern='tmp-bag-%i-of-%i.bag', topics=args.t, reindex=False, start_limit=args.start, end_limit=args.end)
        bagfiles = []
        for i in range(1, 1+args.splits):
            bagfiles.insert(0, "tmp-bag-%i-of-%i_reversed.bag" % (i, args.splits))
            reverse_bag("tmp-bag-%i-of-%i.bag" % (i, args.splits), out_filename=bagfiles[0], start_limit=args.start, end_limit=args.end)
        merge_bag(bagfiles[0], bagfiles[1:], outfile=args.o, topics=None, compression=(rosbag.Compression.BZ2 if args.bz2 else rosbag.Compression.NONE),
                  reindex=False, start_time=args.start, end_time=args.end)
    else:
        reverse_bag(args.bagfile,
                    out_filename=args.o,
                    topics=args.t,
                    compression=(rosbag.Compression.BZ2 if args.bz2 else rosbag.Compression.NONE),
                    start_limit=args.start,
                    end_limit=args.end)
