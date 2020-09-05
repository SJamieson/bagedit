#!/usr/bin/env python
# Adapted from work by Will Greene: https://gist.github.com/wngreene/835cda68ddd9c5416defce876a4d7dd9
# Copyright 2016 Massachusetts Institute of Technology

import os
import argparse

import cv2

import rosbag
from cv_bridge import CvBridge


def save_images(bag, topics, output_dir):
    """Extract a folder of images from a rosbag.
    """
    bridge = CvBridge()
    count = [0 for i in range(len(topics))]
    topic: str
    for topic, msg, t in bag.read_messages(topics=topics):
        cv_img = bridge.imgmsg_to_cv2(msg, desired_encoding="passthrough")

        cv2.imwrite(os.path.join(output_dir, "%06i-%s.png" %
                                 (count[topics.index(topic)], topic[1:].replace("/", "."))), cv_img)
        count[topics.index(topic)] += 1

    bag.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Extract images from a ROS bag.")
    parser.add_argument("bag_file", help="Input ROS bag.")
    parser.add_argument("output_dir", help="Output directory.")
    parser.add_argument("image_topic", nargs="+", help="Image topic.")

    args = parser.parse_args()

    bag = rosbag.Bag(args.bag_file, "r")
    save_images(bag, args.image_topic, args.output_dir)
