#!/usr/bin/env python
# Adapted from the work of Alexander Kasper at
# http://answers.ros.org/question/11537/creating-a-bag-file-out-of-a-image-sequence/?answer=173665#post-id-173665

import sys
from PIL import Image as PILImage
from distutils.util import strtobool
from scipy.io import loadmat

import rosbag
import rospy
from copy import copy
from sensor_msgs.msg import NavSatFix
from geometry_msgs.msg import TransformStamped
from tf.msg import tfMessage
import tf_conversions
from geometry_msgs.msg import Quaternion
from nav_msgs.msg import Odometry
from geodesy.utm import fromLatLong
from tqdm import tqdm
import math


def CreateBag(matfile, bagname, frame_id, navsat_topic="/fix"):
    '''Creates the actual bag file by successively adding images'''
    bag = rosbag.Bag(bagname, 'w', compression=rosbag.Compression.BZ2, chunk_threshold=32 * 1024 * 1024)
    data = loadmat(matfile)['rnv']
    rnv = {}
    for i, col in enumerate(data.dtype.names):
        rnv[col] = data.item()[i]
    try:
        ref = None
        x, y = [], []
        for i, t in enumerate(tqdm(rnv['t'])):
            # print("Adding %s" % image_name)

            stamp = rospy.Time.from_sec(t)
            nav_msg = NavSatFix()
            nav_msg.header.stamp = stamp
            nav_msg.header.frame_id = "map"
            nav_msg.header.seq = i
            nav_msg.latitude = rnv['lat'][i][0]
            nav_msg.longitude = rnv['lon'][i][0]
            nav_msg.altitude = -rnv['depth'][i][0]
            nav_msg.position_covariance_type = nav_msg.COVARIANCE_TYPE_UNKNOWN

            transform_msg = TransformStamped()
            transform_msg.header = copy(nav_msg.header)
            utm = fromLatLong(nav_msg.latitude, nav_msg.longitude, nav_msg.altitude)
            if ref is None:
                ref = utm.toPoint()
            x.append(utm.toPoint().x - ref.x)
            y.append(utm.toPoint().y - ref.y)
            dx = x[-1] - sum(x[-min(20, len(x)):]) / min(20, len(x))
            dy = y[-1] - sum(y[-min(20, len(y)):]) / min(20, len(y))
            transform_msg.transform.translation.x = x[-1]
            transform_msg.transform.translation.y = y[-1]
            transform_msg.transform.translation.z = nav_msg.altitude
            transform_msg.transform.rotation = Quaternion(*tf_conversions.transformations.quaternion_from_euler(0, 0,
                                                                                                    math.atan2(dy, dx)))
            transform_msg.child_frame_id = frame_id
            tf_msg = tfMessage(transforms=[transform_msg])

            odometry_msg = Odometry()
            odometry_msg.header = copy(transform_msg.header)
            odometry_msg.child_frame_id = frame_id
            odometry_msg.pose.pose.position = transform_msg.transform.translation
            odometry_msg.pose.pose.orientation = transform_msg.transform.rotation

            bag.write(navsat_topic, nav_msg, stamp)
            bag.write("/tf", tf_msg, stamp)
            bag.write("/transform", transform_msg, stamp)
            bag.write("/odom", odometry_msg, stamp)
    finally:
        bag.close()


if __name__ == "__main__":
    if len(sys.argv) >= 4:
        CreateBag(*sys.argv[1:])
    else:
        print("Usage: nav2bag.py matfile bagfile frame_id [navsat_topic=/fix]")
