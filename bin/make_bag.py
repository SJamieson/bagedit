#!/usr/bin/env python
# Adapted from the work of Alexander Kasper at http://answers.ros.org/question/11537/creating-a-bag-file-out-of-a-image-sequence/?answer=173665#post-id-173665

import time, sys, os, datetime
import numpy as np
import rosbag
from PIL import Image as PILImage
import rospy
from tqdm import tqdm
from sensor_msgs.msg import Image
from cv_bridge import CvBridge, CvBridgeError
import cv2


def GetFilesFromDir(dir):
    '''Generates a list of files from the directory'''
    print( "Searching directory %s" % dir )
    all = []
    left_files = []
    right_files = []
    if os.path.exists(dir):
        for path, names, files in os.walk(dir):
            for f in sorted(files):
                if os.path.splitext(f)[1] in ['.bmp', '.png', '.jpg', '.tif']:
                    if 'left' in f or 'left' in path:
                        left_files.append( os.path.join( path, f ) )
                    elif 'right' in f or 'right' in path:
                        right_files.append( os.path.join( path, f ) )
                    all.append( os.path.join( path, f ) )
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

def CreateMonoBag(imgs, bagname, time_format=None, scale=1.):
    '''Creates a bag file with camera images'''
    bag =rosbag.Bag(bagname, 'w', compression=rosbag.Compression.BZ2, chunk_threshold=32 * 1024 * 1024)
    bridge = CvBridge()
    try:
        for image_name, i in enumerate(tqdm(imgs)):
            # print("Adding %s" % image_name)

            Stamp = None
            if time_format is None:
                Stamp = rospy.rostime.Time.from_sec(time.time())
            elif time_format == "name.date.time.n.ext":
                tokens = image_name.split('.')
                bag_stamp = datetime.datetime.strptime(':'.join(tokens[1:3]), "%Y%m%d:%H%M%S%f")
                Stamp = rospy.rostime.Time(secs=int(time.mktime(bag_stamp.timetuple())),
                                           nsecs=bag_stamp.microsecond*1000)
                # print(time.mktime(bag_stamp.timetuple()))
                # print(bag_stamp.second, bag_stamp.microsecond*1E3)

            with open(image_name, "rb") as rawimage:
                im = PILImage.open(rawimage)  # type: PILImage.Image
                imcols, imrows = im.size
                imsize = imrows*imcols

            with open(image_name, "rb") as rawimage:
                img = np.fromfile(rawimage, np.dtype('u2'), imsize).reshape((imrows, imcols))
                colour = cv2.cvtColor(img, cv2.COLOR_BAYER_GB2BGR)  # type: np.ndarray

                if scale != 1.:
                    colour = cv2.resize(colour, None, fx=scale, fy=scale, interpolation=cv2.INTER_CUBIC)

                Img = bridge.cv2_to_imgmsg((colour/256).astype('uint8'), "rgb8")  # type: Image
                Img.header.stamp = Stamp
                Img.header.frame_id = "camera"
                Img.header.seq = i

                bag.write('camera/image_raw', Img, Stamp)
    finally:
        bag.close()


def CreateBag(image_dir, bagname, time_format=None, scale=1.):
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
        CreateMonoBag(all_imgs, bagname, time_format=time_format, scale=scale)


if __name__ == "__main__":
    if len( sys.argv ) >= 3:
        CreateBag(*sys.argv[1:])
    else:
        print( "Usage: img2bag imagedir bagfilename <time_format> <scale>")
