#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# License: BSD
#   https://raw.githubusercontent.com/splintered-reality/py_trees_ros/devel/LICENSE
#
##############################################################################
# Documentation
##############################################################################

"""
Assorted utility functions.
"""

##############################################################################
# Imports
##############################################################################

import os
import pathlib
import time
import typing

import rospy
import rosservice

import py_trees_ros_interfaces.msg as py_trees_msgs  # noqa
import py_trees_ros_interfaces.srv as py_trees_srvs  # noqa

from . import exceptions

##############################################################################
# Methods
##############################################################################


# # QUESTION(lindzey): how is this actually used?
# # => Needed by blackboard, to find list/open/close Blackboard services that might be in
# #    different namespaces.
# # => I've looked, and can't find an equivalent to rospy.get_published_topics()
# # => oh! import rosservice; rosservice.get_service_list()
def find_service(
    service_type: str,
    namespace: typing.Optional[str] = None,
    timeout: float = 0.5,
) -> str:
    """
    Discover a service of the specified type and if necessary, under the specified
    namespace.

    Args:
        service_type (:obj:`str`): primary lookup hint
        namespace (:obj:`str`): secondary lookup hint
        timeout: immediately post node creation, can take time to discover the graph (sec)

    Returns:
        :obj:`str`: fully expanded service name

    Raises:
        :class:`~py_trees_ros.exceptions.NotFoundError`: if no services were found
        :class:`~py_trees_ros.exceptions.MultipleFoundError`: if multiple services were found
    """
    # TODO: follow the pattern of ros2cli to create a node without the need to init
    # rcl (might get rid of the magic sleep this way). See:
    #    https://github.com/ros2/ros2cli/blob/master/ros2service/ros2service/verb/list.py
    #    https://github.com/ros2/ros2cli/blob/master/ros2cli/ros2cli/node/strategy.py

    loop_period = 0.1  # seconds
    start_time = rospy.Time.now()
    service_names = []
    while rospy.Time.now() - start_time < rospy.Duration.from_sec(timeout):
        # TODO(lindzey): If we need this function, find the ROS1 equivalent to this!
        #   (it has to be possible, thanks to `rosservice list`)
        # Returns a list of the form: [('exchange/blackboard', ['std_msgs/String'])
        all_service_names = rosservice.get_service_list()
        for service_name in all_service_names:
            tt = rosservice.get_service_type(service_name)
            if service_type == tt:
                service_names.append(service_name)

        if namespace is not None:
            # QUESTION(lindzey): Why isn't this "startswith", rather than "in"?
            service_names = [name for name in service_names if namespace in name]
        if service_names:
            break
        rospy.sleep(loop_period)

    if not service_names:
        raise exceptions.NotFoundError(
            "service not found [type: {}]".format(service_type)
        )
    elif len(service_names) == 1:
        return service_names[0]
    else:
        raise exceptions.MultipleFoundError(
            "multiple services found [type: {}]".format(service_type)
        )


# # NOTE(lindzey): This appears to be unused in the py_trees_ros codebase,
# #    other than a TODO in echo.py to switch to using this.
# # Would probably use rospy.get_published_topics()
# def find_topics(
#     node, topic_type: str, namespace: typing.Optional[str] = None, timeout: float = 0.5
# ) -> typing.List[str]:
#     """
#     Discover a topic of the specified type and if necessary, under the specified
#     namespace.

#     Args:
#         node: nodes have the discovery methods
#         topic_type: primary lookup hint
#         namespace: secondary lookup hint
#         timeout: check every 0.1s until this timeout is reached (can be None -> checks once)

#     .. note: Immediately post node creation, it can take some time to discover the graph.

#     Returns:
#         list of fully expanded topic names (can be empty)
#     """
#     # TODO: follow the pattern of ros2cli to create a node without the need to init
#     # rcl (might get rid of the magic sleep this way). See:
#     #    https://github.com/ros2/ros2cli/blob/master/ros2service/ros2service/verb/list.py
#     #    https://github.com/ros2/ros2cli/blob/master/ros2cli/ros2cli/node/strategy.py
#     loop_period = 0.1  # seconds
#     clock = rclpy.clock.Clock()
#     start_time = clock.now()
#     topic_names = []
#     while True:
#         # Returns a list of the form: [('exchange/blackboard', ['std_msgs/String'])
#         topic_names_and_types = node.get_topic_names_and_types()
#         topic_names = [
#             name for name, types in topic_names_and_types if topic_type in types
#         ]
#         if namespace is not None:
#             topic_names = [name for name in topic_names if namespace in name]
#         if topic_names:
#             break
#         if timeout is None or (clock.now() - start_time) > rclpy.time.Duration(
#             seconds=timeout
#         ):
#             break
#         else:
#             time.sleep(loop_period)
#     return topic_names


def basename(name):
    """
    Generate the basename from a ros name.

    Args:
        name (:obj:`str`): ros name

    Returns:
        :obj:`str`: name stripped up until the last slash or tilde character.
    Examples:

        .. code-block:: python

           basename("~dude")
           # 'dude'
           basename("/gang/dude")
           # 'dude'
    """
    return name.rsplit("/", 1)[-1].rsplit("~", 1)[-1]


# # Commenting out because the comments suggest that in ROS1, this is just rospkg.get_ros_home()
# def get_py_trees_home():
#     """
#     Find the default home directory used for logging, bagging and other
#     esoterica.
#     """
#     # TODO: update with replacement for rospkg.get_ros_home() when it arrives
#     home = os.path.join(str(pathlib.Path.home()), ".ros2", "py_trees")
#     return home


# # TODO(lindzey): I think these can just be removed for ROS1
# def qos_profile_latched():
#     """
#     Convenience retrieval for a latched topic (publisher / subscriber)
#     """
#     return rclpy.qos.QoSProfile(
#         history=rclpy.qos.QoSHistoryPolicy.KEEP_LAST,
#         depth=1,
#         durability=rclpy.qos.QoSDurabilityPolicy.TRANSIENT_LOCAL,
#         reliability=rclpy.qos.QoSReliabilityPolicy.RELIABLE,
#     )


# def qos_profile_unlatched():
#     """
#     Default profile for an unlatched topic (in py_trees_ros).
#     """
#     return rclpy.qos.QoSProfile(
#         history=rclpy.qos.QoSHistoryPolicy.KEEP_LAST,
#         depth=1,
#         durability=rclpy.qos.QoSDurabilityPolicy.VOLATILE,
#         reliability=rclpy.qos.QoSReliabilityPolicy.RELIABLE,
#     )

# # Not needed in ROS1, since you can just use publisher.resolved_name
# def resolve_name(node, name):
#     """
#     Convenience function for getting the resolved name (similar to 'publisher.resolved_name' in ROS1).

#     Args:
#         node (:class:`rclpy.node.Node`): the node, namespace it *should* be relevant to
#         name (obj:`str`): topic or service name

#     .. note::

#        This entirely depends on the user providing the relevant node, name pair.
#     """
#     return rclpy.expand_topic_name.expand_topic_name(
#         name, node.get_name(), node.get_namespace()
#     )


def create_anonymous_node_name(node_name="node") -> str:
    """
    Creates an anonoymous node name by adding a suffix created from
    a monotonic timestamp, sans the decimal.

    Returns:
        :obj:`str`: the unique, anonymous node name
    """
    return node_name + "_" + str(time.monotonic()).replace(".", "")


##############################################################################
# Convenience Classes
##############################################################################


class Publishers(object):
    """
    Utility class that groups the publishers together in one convenient structure.

    Args:
        publisher_details (obj:`tuple`): list of (str, str, msgType, bool, int) tuples representing
                                  (unique_name, topic_name, publisher_type, latched)
                                  specifications for creating publishers

    Examples:
        Convert the incoming list of specifications into proper variables of this class.

        .. code-block:: python

           publishers = py_trees.utilities.Publishers(
               [
                   ('foo', '~/foo', std_msgs.String, True, 5),
                   ('bar', '/foo/bar', std_msgs.String, False, 5),
                   ('foobar', '/foo/bar', std_msgs.String, False, 5),
               ]
           )
    """

    def __init__(
        self, publisher_details, introspection_topic_name="publishers"
    ) -> None:
        # TODO: check for the correct setting of publisher_details
        self.publisher_details_msg = []
        for name, topic_name, publisher_type, latched in publisher_details:
            pub = rospy.Publisher(
                topic_name,
                publisher_type,
                latch=latched,
                queue_size=1,
            )
            self.__dict__[name] = pub
            resolved_name = pub.resolved_name
            message_type = (
                publisher_type.__class__.__module__.split(".")[0]
                + "/"
                + publisher_type.__class__.__name__
            )
            self.publisher_details_msg.append(
                py_trees_msgs.PublisherDetails(
                    topic_name=resolved_name, message_type=message_type, latched=latched
                )
            )

        self.introspection_service = rospy.Service(
            "~/introspection/" + introspection_topic_name,
            py_trees_srvs.IntrospectPublishers,
            self.introspection_callback,
        )

    def introspection_callback(self, unused_request):
        response = py_trees_srvs.IntrospectPublishersResponse()
        response.publisher_details = self.publisher_details_msg
        return response


# class Subscribers(object):
#     """
#     Utility class that groups subscribers together in one convenient structure.

#     Args:
#         subscriber_details (obj:`tuple`): list of (str, str, msgType, bool, func) tuples representing
#                                   (unique_name, topic_name, subscriber_type, latched, callback)
#                                   specifications for creating subscribers

#     Examples:
#         Convert the incoming list of specifications into proper variables of this class.

#         .. code-block:: python

#            subscribers = py_trees.utilities.Subscribers(
#                [
#                    ('foo', '~/foo', std_msgs.String, True, foo),
#                    ('bar', '/foo/bar', std_msgs.String, False, self.foo),
#                    ('foobar', '/foo/bar', std_msgs.String, False, foo.bar),
#                ]
#            )
#     """

#     def __init__(
#         self, subscriber_details, introspection_topic_name="subscribers"
#     ) -> None:
#         # TODO: check for the correct setting of subscriber_details
#         self.subscriber_details_msgs = []
#         for name, topic_name, subscriber_type, latched, callback in subscriber_details:
#             if latched:
#                 self.__dict__[name] = node.create_subscription(
#                     msg_type=subscriber_type,
#                     topic=topic_name,
#                     callback=callback,
#                     qos_profile=qos_profile_latched(),
#                 )
#             else:
#                 self.__dict__[name] = node.create_subscription(
#                     msg_type=subscriber_type,
#                     topic=topic_name,
#                     callback=callback,
#                     qos_profile=qos_profile_unlatched(),
#                 )

#             resolved_name = resolve_name(node, topic_name)
#             message_type = (
#                 subscriber_type.__class__.__module__.split(".")[0]
#                 + "/"
#                 + subscriber_type.__class__.__name__
#             )
#             self.subscriber_details_msgs.append(
#                 py_trees_msgs.SubscriberDetails(
#                     topic_name=resolved_name, message_type=message_type, latched=latched
#                 )
#             )

#         self.introspection_service = rospy.Service(
#             "~/introspection/" + introspection_topic_name,
#             py_trees_srvs.IntrospectSubscribers,
#             self.introspection_callback,
#         )

#     def introspection_callback(self, unused_request):
#         response = py_tree_srvs.IntrospectSubscribersResponse()
#         response.subscriber_details = self.subscriber_details_msgs
#         return response


class Services(object):
    """
    Utility class that groups services together in one convenient structure.

    Args:
        service_details (obj:`tuple`): list of (str, str, srvType, func) tuples representing
                                  (unique_name, topic_name, service_type, callback)
                                  specifications for creating services

    Examples:
        Convert the incoming list of specifications into proper variables of this class.

        .. code-block:: python

           services = py_trees.utilities.Services(
               [
                   ('open_foo', '~/get_foo', foo_interfaces.srv.OpenFoo, open_foo_callback),
                   ('open_foo', '/foo/open', foo_interfaces.srv.OpenFoo, self.open_foo_callback),
                   ('get_foo_bar', '/foo/bar', foo_interfaces.srv.GetBar, self.foo.get_bar_callback),
               ]
           )
    """

    def __init__(self, service_details, introspection_topic_name="services"):
        # TODO: check for the correct setting of subscriber_details
        self.service_details_msg = []
        for name, service_name, service_type, callback in service_details:
            self.__dict__[name] = rospy.Service(service_name, service_type, callback)
            resolved_name = self.__dict__[name].resolved_name
            service_type = (
                service_type.__class__.__module__.split(".")[0]
                + "/"
                + service_type.__class__.__name__
            )
            self.service_details_msg.append(
                py_trees_msgs.ServiceDetails(
                    service_name=resolved_name,
                    service_type=service_type,
                )
            )

        self.introspection_service = rospy.Service(
            "~/introspection/" + introspection_topic_name,
            py_trees_srvs.IntrospectServices,
            self.introspection_callback,
        )

    def introspection_callback(self, unused_request):
        response = py_trees_srvs.IntrospectServicesResponse()
        response.subscriber_details = self.subscriber_details_msg
        return response
