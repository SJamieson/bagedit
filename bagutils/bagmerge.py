#!/usr/bin/env python
import rosbag
from rospy import rostime
from tqdm import tqdm
import argparse
import os


def parse_args():
    parser = argparse.ArgumentParser(
        prog='bagmerge.py',
        description='Merges two bagfiles.')
    parser.add_argument('-o', type=str, help='name of the output file',
                        default=None, metavar="output_file")
    parser.add_argument('-t', type=str, help='topics which should be merged to the main bag',
                        default=None, metavar="topics")
    parser.add_argument('-i', help='reindex bagfile',
                        default=False, action="store_true")
    parser.add_argument('main_bagfile', type=str, help='path to a bagfile, which will be the main bagfile')
    parser.add_argument('bagfiles', nargs='+', type=str,
                        help='path to the bagfile(s) which should be merged to the main bagfile')
    parser.add_argument('--start', type=float, help='start time', default=float('-inf'), metavar="start")
    parser.add_argument('--end', type=float, help='end time', default=float('inf'), metavar="end")
    parser.add_argument('--bz2', help='bz2 compression', default=False, action="store_true")
    args = parser.parse_args()
    return args


def get_next(bag_iter, reindex=False,
             main_start_time=None, start_time=None,
             topics=None):
    """
    :type bag_iter: rosbag.Bag
    """
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
    except AttributeError:
        return None


def merge_bag(main_bagfile, bagfiles, outfile=None, topics=None, compression=rosbag.Compression.NONE,
              reindex=True, start_time=float('-inf'), end_time=float('inf')):
    # get min and max time in bagfile
    bagfiles = [main_bagfile] + bagfiles
    limits = [get_limits(bagfile) for bagfile in bagfiles]
    start_limit = max(start_time, min(limits, key=lambda x: x[0])[0])
    end_limit = min(end_time, max(limits, key=lambda x: x[1])[1])
    # check output file
    if outfile is None:
        pattern = main_bagfile + "_merged_%i.bag"
        outfile = main_bagfile + "_merged.bag"
        index = 0
        while (os.path.exists(outfile)):
            outfile = pattern % index
            index += 1
    # output some information
    # print "merge bag %s in %s" % (bagfile, main_bagfile)
    print("topics filter: ", topics)
    print("writing to %s." % outfile)
    # merge bagfile
    outbag = rosbag.Bag(outfile, 'w', compression=compression, chunk_threshold=32 * 1024 * 1024)
    bags = []
    for i in range(len(bagfiles)):
        if i > 0 and (limits[i][0] > end_limit or limits[i][1] < start_limit):
            print('Skipping ' + bagfiles[i])
            bags.append(None)
            continue
        bags.append(rosbag.Bag(bagfiles[i], 'r', skip_index=False, chunk_threshold=8 * 1024 * 1024))
        bags[-1].close()
        bags[-1]._open_read(bags[-1]._filename, False)
        bags[-1] = bags[-1].__iter__()
    next = [None] * len(bagfiles)

    def find_next(next):
        i = 0
        next_time = float('inf')
        for n in range(len(next)):
            if next[n] is None:
                continue
            test_time = next[n][2].to_sec()
            if test_time < next_time:
                i = n
                next_time = test_time
        return i, next_time

    def update_next(next, i):
        if i == 0:
            next[0] = get_next(bags[i])
        else:
            next[i] = get_next(bags[i], reindex, limits[0], limits[i][0], topics)

    for i in range(len(bagfiles)):
        update_next(next, i)
    try:
        print('Beginning write.')
        progress = tqdm(total=end_limit - start_limit, unit='s', unit_scale=True)
        prev_time = start_limit
        next_i, next_time = find_next(next)  # type: (int, Union[float, rostime.Time])
        while next_time <= end_limit:
            if next_time >= start_limit:
                outbag.write(next[next_i][0], next[next_i][1], next[next_i][2])
                progress.update(max(0, next_time - prev_time))
                if prev_time > next_time:
                    print("Prev time:", str(prev_time), "Next time:", str(next_time))
                prev_time = next_time
            update_next(next, next_i)
            next_i, next_time = find_next(next)
        progress.close()
    finally:
        outbag.close()


def get_limits(bagfile, validate=False):
    print("Determine start and end index of %s..." % bagfile)
    bag = rosbag.Bag(bagfile, 'r', skip_index=True)
    try:
        start_time = bag.get_start_time()
        end_time = bag.get_end_time()
    except:
        start_time = float("-inf")
        end_time = float("inf")
    if validate:
        _end_time = None
        _start_time = None

        for topic, msg, t in bag.read_messages():
            if _start_time == None or t < _start_time:
                _start_time = t
            if _end_time == None or t > _end_time:
                _end_time = t
        assert (start_time == _start_time.to_sec())
        assert (end_time == _end_time.to_sec())
        print('validated')
    return (start_time, end_time)


if __name__ == "__main__":
    args = parse_args()
    # print args
    if args.t != None:
        args.t = args.t.split(',')
    merge_bag(args.main_bagfile,
              args.bagfiles,
              outfile=args.o,
              topics=args.t,
              compression=(rosbag.Compression.BZ2 if args.bz2 else rosbag.Compression.NONE),
              reindex=args.i,
              start_time=args.start,
              end_time=args.end)
