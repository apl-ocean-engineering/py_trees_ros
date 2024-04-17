cmake_minimum_required(VERSION 3.0.2)
project(py_trees_ros)

add_compile_options(-std=c++17)

find_package(catkin REQUIRED COMPONENTS
  actionlib
  rospy
  actionlib_msgs
  geometry_msgs
  sensor_msgs
  std_msgs
  uuid_msgs
)

catkin_python_setup()
