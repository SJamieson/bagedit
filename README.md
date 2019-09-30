Catkin version of https://bitbucket.org/daniel_dube/bagedit, which are scripts to manage ROS bag files.

This is a small collection of scripts to manage ROS bag files. You can trim a bag ang merge two bages 

# Getting started
* Checkout the bagedit node to catkin workspace folder:
```
cd ~/catkin_ws/src
git clone https://github.com/MHarbi/bagedit.git
cd ~/catkin_ws
cd ~/catkin_make
```

* Run the script:
```
rosrun bagedit bagtrim.py --help
rosrun bagedit bagmerge.py --help
```

# bagtrim
```
usage: bagtrim.py [-h] [-s start_time] [-e end_time] [-o output_file] [-a]
                  bagfile

Trims the beginning and the and of a bagfile.

positional arguments:
  bagfile         path to a bagfile

optional arguments:
  -h, --help      show this help message and exit
  -s start_time   start time in seconds
  -e end_time     end time in seconds
  -o output_file  name of the output file
  -a              use absolute timestamps
```

# bagmerge
```
usage: bagmerge.py [-h] [-o output_file] [-t topics] [-i] main_bagfile bagfile

Merges two bagfiles.

positional arguments:
  main_bagfile    path to a bagfile, which will be the main bagfile
  bagfile         path to a bagfile which should be merged to the main bagfile

optional arguments:
  -h, --help      show this help message and exit
  -o output_file  name of the output file
  -t topics       topics which should be merged to the main bag
  -i              reindex bagfile
```

# img2bag 
```
usage: img2bag.py image_dir bag_filename is_bayered [time_format] [scale=1]

Creates a bagfile out of a directory of images.

positional arguments:
  image_dir            directory which contains images
  bag_filename         name of output bagfile
  is_bayered           true if the input images need to be debayered, false otherwise

optional arguments:
  time_format          at the moment, only supports being unset or equal to "name.date.time.n.ext" if that is the filename format of the input images; if so, will set timestamp of each message accordingly
  scale                scales input images in both dimensions by a constant factor, using cubic interpolation
```
