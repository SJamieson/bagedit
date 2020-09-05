#!/usr/bin/env python
# Adapted from the work of Alexander Kasper at
# http://answers.ros.org/question/11537/creating-a-bag-file-out-of-a-image-sequence/?answer=173665#post-id-173665

import cv2
import datetime
import os
import sys
import time
from PIL import Image as PILImage
from distutils.util import strtobool

import numpy as np
import rosbag
import rospy
from cv_bridge import CvBridge
from sensor_msgs.msg import Image
from tqdm import tqdm


def LoadImage(image_name, scale, debayer=False):
    """
    :returns np.ndarray containing an 8-bit RGB image
    """
    with open(image_name, "rb") as rawimage:
        im = PILImage.open(rawimage)  # type: PILImage.Image

        if debayer:
            print("Attempting to debayer image")
            assert (len(im.size) == 2 or im.size[2] == 1)
            # imgMat = np.fromfile(rawimage, np.dtype('u2'), imsize).reshape((imrows, imcols))
            img = np.array(im, dtype='u2')
            img = cv2.cvtColor(img, cv2.COLOR_BayerGB2BGR)  # type: np.ndarray
            img = (img * (255. / img.max())).astype('uint8')
        else:
            img = np.array(im, dtype='uint8')

        if scale != 1.:
            img = cv2.resize(img, None, fx=scale, fy=scale, interpolation=cv2.INTER_CUBIC)

        return img


def GetFilesFromDir(dir):
    '''Generates a list of files from the directory'''
    print("Searching directory %s" % dir)
    all = []
    left_files = []
    right_files = []
    if os.path.exists(dir):
        for path, names, files in os.walk(dir):
            for f in sorted(files):
                if os.path.splitext(f)[1] in ['.bmp', '.png', '.jpg', '.tif']:
                    if 'left' in f or 'left' in path:
                        left_files.append(os.path.join(path, f))
                    elif 'right' in f or 'right' in path:
                        right_files.append(os.path.join(path, f))
                    all.append(os.path.join(path, f))
    return all, left_files, right_files


# def CreateStereoBag(left_imgs, right_imgs, bagname, time_format=None):
#     '''Creates a bag file containing stereo image pairs'''
#     bag =rosbag.Bag(bagname, 'w')
#
#     try:
#         for i in range(len(left_imgs)):
#             print("Adding %s" % left_imgs[i])
#             fp_left = open( left_imgs[i], "r" )
#             p_left = ImageFile.Parser()
#
#             while 1:
#                 s = fp_left.read(1024)
#                 if not s:
#                     break
#                 p_left.feed(s)
#
#             im_left = p_left.close()
#
#             fp_right = open( right_imgs[i], "r" )
#             print("Adding %s" % right_imgs[i])
#             p_right = ImageFile.Parser()
#
#             while 1:
#                 s = fp_right.read(1024)
#                 if not s:
#                     break
#                 p_right.feed(s)
#
#             im_right = p_right.close()
#
#             assert time_format is None
#             Stamp = roslib.rostime.Time.from_sec(time.time())
#
#             Img_left = Image()
#             Img_left.header.stamp = Stamp
#             Img_left.width = im_left.size[0]
#             Img_left.height = im_left.size[1]
#             Img_left.encoding = "rgb8"
#             Img_left.header.frame_id = "camera/left"
#             Img_left_data = [pix for pixdata in im_left.getdata() for pix in pixdata]
#             Img_left.data = Img_left_data
#             Img_right = Image()
#             Img_right.header.stamp = Stamp
#             Img_right.width = im_right.size[0]
#             Img_right.height = im_right.size[1]
#             Img_right.encoding = "rgb8"
#             Img_right.header.frame_id = "camera/right"
#             Img_right_data = [pix for pixdata in im_right.getdata() for pix in pixdata]
#             Img_right.data = Img_right_data
#
#             bag.write('camera/left/image_raw', Img_left, Stamp)
#             bag.write('camera/right/image_raw', Img_right, Stamp)
#     finally:
#         bag.close()


def CreateMonoBag(imgs, bagname, time_format=None, scale=1., bayered=False):
    '''Creates a bag file with camera images'''
    bag = rosbag.Bag(bagname, 'w', compression=rosbag.Compression.BZ2, chunk_threshold=32 * 1024 * 1024)
    bridge = CvBridge()
    try:
        for i, image_path in enumerate(tqdm(imgs)):
            # print("Adding %s" % image_path)

            try:
                Stamp = None
                image_name = os.path.basename(image_path)
                if time_format is None or time_format == "":
                    Stamp = rospy.rostime.Time.from_sec(1574283415 + i)
                elif time_format == "name.date.time.n.ext" or time_format == "sentry503":
                    # format is name.date.time.n.ext where time is hours/minutes/seconds/microseconds
                    tokens = image_name.split('.')
                    bag_stamp = datetime.datetime.strptime(':'.join(tokens[1:3]), "%Y%m%d:%H%M%S%f")
                    Stamp = rospy.rostime.Time(secs=int(time.mktime(bag_stamp.timetuple())),
                                               nsecs=int(bag_stamp.microsecond * 1E3))
                    # print(time.mktime(bag_stamp.timetuple()))
                    # print(bag_stamp.second, bag_stamp.microsecond*1E3)
                elif time_format == "201504_Panama":
                    # format is date.time.ms.n.ext where time is hours/minutes/seconds and ms is milliseconds
                    tokens = image_name.split('.')
                    bag_stamp = datetime.datetime.strptime(':'.join(tokens[:2]), "%Y%m%d:%H%M%S")
                    Stamp = rospy.rostime.Time(secs=int(time.mktime(bag_stamp.timetuple())),
                                               nsecs=int(int(tokens[2]) * 1E6))

                img = LoadImage(image_path, scale, debayer=bayered)

                ImgMsg = bridge.cv2_to_imgmsg(img, "rgb8")  # type: Image
                ImgMsg.header.stamp = Stamp
                ImgMsg.header.frame_id = "camera"
                ImgMsg.header.seq = i
            except ValueError:
                print("WARNING: Failed to process file %s" % image_path)
                continue

            bag.write('camera/image_raw', ImgMsg, Stamp)
    finally:
        bag.close()


def CreateBag(image_dir, bagname, bayered, time_format=None, scale=1.):
    '''Creates the actual bag file by successively adding images'''
    all_imgs, left_imgs, right_imgs = GetFilesFromDir(image_dir)
    if len(all_imgs) <= 0:
        print("No images found in %s" % image_dir)
        exit()

    if len(left_imgs) > 0 and len(right_imgs) > 0:
        # create bagfile with stereo camera image pairs
        # CreateStereoBag(left_imgs, right_imgs, bagname, time_format=time_format)
        raise NotImplementedError("Disabled")
    else:
        # create bagfile with mono camera image stream
        CreateMonoBag(all_imgs, bagname, time_format=time_format, scale=float(scale), bayered=strtobool(bayered))


if __name__ == "__main__":
    if len(sys.argv) >= 4:
        CreateBag(*sys.argv[1:])
    else:
        print("Usage: img2bag.py imagedir bagfilename bayered <time_format> <scale>")
    if len(sys.argv) == 3:
        print(sys.argv[2], strtobool(sys.argv[2]))
        img = LoadImage(sys.argv[1], 1., debayer=strtobool(sys.argv[2]))
        pilImg = PILImage.fromarray(img)
        pilImg.show()
