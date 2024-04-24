#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# License: BSD
#   https://raw.github.com/splintered-reality/py_trees_ros/license/LICENSE
#
##############################################################################
# Documentation
##############################################################################

"""
The :class:`py_trees_ros.trees.BehaviourTree` class
extends the core :class:`py_trees.trees.BehaviourTree` class
with both blackboard and (tree) snapshot streaming services.
Interact with these services via the :ref:`py-trees-blackboard-watcher` and
:ref:`py-trees-tree-watcher` command line utilities.
"""

##############################################################################
# Imports
##############################################################################

import collections
import enum
import functools
import math
import os
import statistics
import subprocess
import tempfile
import time
import typing
import uuid
from typing import Deque, Dict, Iterable, Optional

import diagnostic_msgs.msg as diagnostic_msgs  # noqa
import dynamic_reconfigure
import dynamic_reconfigure.server
import py_trees
import py_trees.common
import py_trees.console as console
import rospy
import uuid_msgs.msg

import py_trees_ros_interfaces.msg as py_trees_msgs  # noqa
import py_trees_ros_interfaces.srv as py_trees_srvs  # noqa
from py_trees_ros.cfg import BehavioursConfig

from . import blackboard, conversions, exceptions, utilities, visitors

##############################################################################
# Tree Management
##############################################################################


def make_topic_name(namespace: str, node_name: str, topic_name) -> str:
    """
    This is an attempt to reproduce the logic in github.com/ros2/rcl/src/rcl/expand_topic_name.c
    """
    rospy.logwarn(
        f"make_topic_name(namespace={namespace}, node_name={node_name}, topic_name={topic_name})"
    )
    is_absolute = topic_name[0] == "/"
    has_tilde = topic_name[0] == "~"
    rospy.logwarn(f"... is_absolute = {is_absolute}")
    rospy.logwarn(f"... has_tilde = {has_tilde}")

    if node_name[0] == "/":
        node_name = node_name[1:]

    if is_absolute:
        rospy.logwarn(f"... ln 72 returning {topic_name}")
        return topic_name
    if has_tilde:
        if len(namespace) == 1:
            fmt = "{}{}{}"
        else:
            fmt = "{}/{}{}"
        local_output = fmt.format(namespace, node_name, topic_name[1:])
    if local_output[0] == "/":
        rospy.logwarn(f"... ln 81 returning {local_output}")
        return local_output
    else:
        if len(namespace) == 1:
            fmt = "{}{}{}"
        else:
            fmt = "{}/{}{}"
        local_output = fmt.format(namespace, node_name, local_output)
        rospy.logwarn(f"... ln 93 returning {local_output}")
    return local_output


class SnapshotStream(object):
    """
    SnapshotStream instances are responsible for creating / destroying
    a snapshot stream as well as the configurable curation of snapshots
    that are published on it.
    """

    _counter = 0
    """Incremental counter guaranteeing unique watcher names"""

    class Parameters(object):
        """
        Reconfigurable parameters for the snapshot stream.

        Args:
            blackboard_data: publish blackboard variables on the visited path
            blackboard_activity: enable and publish blackboard activity in the last tick
            snapshot_period: period between snapshots (use /inf to only publish on tree status changes)
        """

        def __init__(
            self,
            blackboard_data: bool = False,
            blackboard_activity: bool = False,
            snapshot_period: float = py_trees.common.Duration.INFINITE,
        ):
            self.blackboard_data = blackboard_data
            self.blackboard_activity = blackboard_activity
            self.snapshot_period = (
                py_trees.common.Duration.INFINITE.value
                if snapshot_period == py_trees.common.Duration.INFINITE
                else snapshot_period
            )

    def __init__(
        self,
        topic_name: Optional[str] = None,
        parameters: Optional["SnapshotStream.Parameters"] = None,
    ):
        """
        Create the publisher, ready for streaming.

        Args:
            node: node to hook ros communications on
            topic_name: snapshot stream name, uniquely generated if None
            parameters: configuration of the snapshot stream
        """
        rospy.logwarn(f"ln 134: calling expand_topic_name with {topic_name}")
        self.topic_name = SnapshotStream.expand_topic_name(topic_name)
        self.publisher = rospy.Publisher(
            self.topic_name,
            py_trees_msgs.BehaviourTree,
            queue_size=10,
        )
        self.parameters = (
            parameters if parameters is not None else SnapshotStream.Parameters()
        )
        self.last_snapshot_timestamp: Optional[float] = None
        self.statistics: Optional[py_trees_msgs.Statistics] = None

    @staticmethod
    def expand_topic_name(topic_name: Optional[str]) -> str:
        """
        Custom name expansion depending on the topic name provided. This is part of the
        stream configuration on request which either provides no hint (automatic name
        generation), a simple hint (relative name expanded under the snapshot streams
        namespace) or a complete hint (absolute name that does not need expansion).

        Args:
            topic_name: hint for the topic name

        Returns:
            the expanded topic name
        """
        if topic_name is None or not topic_name:
            # expanded_topic_name = rclpy.expand_topic_name.expand_topic_name(
            #     topic_name="~/snapshot_streams/_snapshots_"
            #     + str(SnapshotStream._counter),
            #     node_name=node.get_name(),
            #     node_namespace=node.get_namespace(),
            # )
            topic_name = "~/snapshot_streams/_snapshots_" + str(SnapshotStream._counter)
            expanded_topic_name = make_topic_name(
                rospy.get_namespace(), rospy.get_name(), topic_name
            )
            rospy.logwarn("ln 171: Expanded name to: %s".format(expanded_topic_name))

            SnapshotStream._counter += 1
            return expanded_topic_name
        elif topic_name.startswith("~"):
            # return rclpy.expand_topic_name.expand_topic_name(
            #     topic_name=topic_name,
            #     node_name=node.get_name(),
            #     node_namespace=node.get_namespace(),
            # )
            expanded_topic_name = make_topic_name(
                rospy.get_namespace(), rospy.get_name(), topic_name
            )
            rospy.logwarn("ln 184: Expanded name to: %s".format(expanded_topic_name))
            return expanded_topic_name
        elif not topic_name.startswith("/"):
            # return rclpy.expand_topic_name.expand_topic_name(
            #     topic_name="~/snapshot_streams/" + topic_name,
            #     node_name=node.get_name(),
            #     node_namespace=node.get_namespace(),
            # )
            rospy.loginfo(f"ln 192: topic_name = {topic_name}")
            topic_name = "~/snapshot_streams/" + topic_name
            rospy.loginfo(f"ln 194: topic_name = {topic_name}")
            rospy.loginfo(f"ln 195: namespace = {rospy.get_namespace()}")
            rospy.loginfo(f"ln 196: nodename = {rospy.get_name()}")
            expanded_topic_name = make_topic_name(
                rospy.get_namespace(), rospy.get_name(), topic_name
            )
            rospy.logwarn("ln 198: Expanded name to: %s".format(expanded_topic_name))
            return expanded_topic_name
        else:
            return topic_name

    def publish(
        self,
        root: py_trees.behaviour.Behaviour,
        changed: bool,
        statistics: py_trees_msgs.Statistics,
        visited_behaviour_ids: typing.Set[uuid.UUID],
        visited_blackboard_client_ids: typing.Set[uuid.UUID],
    ):
        """ "
        Publish a snapshot, including only what has been parameterised.

        Args:
            root: the tree
            changed: whether the tree status / graph changed or not
            statistics: add tree statistics to the snapshot data
            visited_behaviour_ids: behaviours on the visited path
            visited_blackboard_client_ids: blackboard clients belonging to behaviours on the visited path
        """
        if self.last_snapshot_timestamp is None:
            changed = True
            self.last_snapshot_timestamp = time.monotonic()
        current_timestamp = time.monotonic()
        elapsed_time = current_timestamp - self.last_snapshot_timestamp
        # https://github.com/splintered-reality/py_trees_ros/issues/144
        if_its_close_enough = 0.98
        if (
            not changed
            and elapsed_time < if_its_close_enough * self.parameters.snapshot_period
        ):
            return

        tree_message = py_trees_msgs.BehaviourTree()
        tree_message.changed = changed

        # tree
        for behaviour in root.iterate():
            msg = conversions.behaviour_to_msg(behaviour)
            msg.is_active = True if behaviour.id in visited_behaviour_ids else False
            tree_message.behaviours.append(msg)

        # blackboard
        if self.parameters.blackboard_data:
            visited_keys = py_trees.blackboard.Blackboard.keys_filtered_by_clients(
                client_ids=visited_blackboard_client_ids
            )
            for key in visited_keys:
                try:
                    value = str(py_trees.blackboard.Blackboard.get(key))
                except KeyError:
                    value = "-"
                tree_message.blackboard_on_visited_path.append(
                    diagnostic_msgs.KeyValue(key=key, value=value)
                )

        # activity stream
        #   TODO: checking the stream is not None is redundant, perhaps use it as an exception check?
        if (
            self.parameters.blackboard_activity
            and py_trees.blackboard.Blackboard.activity_stream is not None
        ):
            tree_message.blackboard_activity = conversions.activity_stream_to_msgs()
        # other
        if statistics is not None:
            tree_message.statistics = statistics
        self.publisher.publish(tree_message)
        self.last_snapshot_timestamp = current_timestamp

    def shutdown(self):
        """
        Shutdown the (temporarily) created publisher.
        """
        self.publisher.unregister()


class BehaviourTree(py_trees.trees.BehaviourTree):
    """
    Extend the :class:`py_trees.trees.BehaviourTree` class with
    a few bells and whistles for ROS.

    ROS Parameters:
        * **default_snapshot_stream**: enable/disable the default snapshots stream in ~/snapshots` (default: False)
        * **default_snapshot_period**: periodically publish (default: :data:`math.inf`)
        * **default_snapshot_blackboard_data**: include tracking and visited variables (default: True)
        * **default_snapshot_blackboard_activity**: include the blackboard activity (default: False)
        * **setup_timeout**: time (s) to wait (default: :data:`math.inf`)

          * if :data:`math.inf`, it will block indefinitely

    ROS Publishers:
        * **~/snapshots** (:class:`py_trees_interfaces.msg.BehaviourTree`): the default snapshot stream, if enabled

    ROS Services:
        * **~/blackboard_streams/close** (:class:`py_trees_ros_interfaces.srv.CloselackboardWatcher`)
        * **~/blackboard_streams/get_variables** (:class:`py_trees_ros_interfaces.srv.GetBlackboardVariables`)
        * **~/blackboard_streams/open** (:class:`py_trees_ros_interfaces.srv.OpenBlackboardWatcher`)
        * **~/snapshot_streams/close** (:class:`py_trees_ros_interfaces.srv.CloseSnapshotsStream`)
        * **~/snapshot_streams/open** (:class:`py_trees_ros_interfaces.srv.OpenSnapshotsStream`)
        * **~/snapshot_streams/reconfigure** (:class:`py_trees_ros_interfaces.srv.ReconfigureSnapshotsStream`)

    Topics and services are not intended for direct use, but facilitate the operation of the
    utilities :ref:`py-trees-tree-watcher` and :ref:`py-trees-blackboard-watcher`.

    Args:
        root: root node of the tree
        unicode_tree_debug: print to console the visited ascii tree after every tick

    Raises:
        AssertionError: if incoming root variable is not the correct type
    """

    def __init__(
        self, root: py_trees.behaviour.Behaviour, unicode_tree_debug: bool = False
    ):
        super(BehaviourTree, self).__init__(root)
        if unicode_tree_debug:
            self.snapshot_visitor = py_trees.visitors.DisplaySnapshotVisitor()
        else:
            self.snapshot_visitor = py_trees.visitors.SnapshotVisitor()
        self.visitors.append(self.snapshot_visitor)

        self.statistics: Optional[py_trees_msgs.Statistics] = None
        self.time_series: Deque[float] = collections.deque([])
        self.tick_interval_series: Deque[float] = collections.deque([])
        self.tick_duration_series: Deque[float] = collections.deque([])

        self.pre_tick_handlers.append(self._statistics_pre_tick_handler)
        self.post_tick_handlers.append(self._statistics_post_tick_handler)

        self.timer = None

        # delay ROS artifacts so we can construct the tree without a ROS connection
        self.snapshot_streams: Dict[str, SnapshotStream] = {}

        # Flag for whether setup() has been called
        self.is_setup = False

    # The ROS2 option allowed passing in a node or a node name; we're going to assume
    # that a ros node has been created
    def setup(
        self,
        timeout: float = py_trees.common.Duration.INFINITE,
        visitor: Optional[py_trees.visitors.VisitorBase] = None,
        **kwargs: int,
    ):
        """
        Setup the publishers, exchange and add ROS relevant pre/post tick handlers to the tree.
        Ultimately relays this call down to all the behaviours in the tree.

        Args:
            timeout: time (s) to wait (use common.Duration.INFINITE to block indefinitely)
            visitor: runnable entities on each node after it's setup
            **kwargs: distribute args to this behaviour and in turn, to it's children

        .. note:

           This method declares parameters for the snapshot_period and setup_timeout.
           These parameters take precedence over the period and timeout args provided here.
           If parameters are not configured at runtime, then the period and timeout args
           provided here will initialise the declared parameters.

        Raises:
            rclpy.exceptions.NotInitializedException: rclpy not yet initialised
            Exception: be ready to catch if any of the behaviours raise an exception
        """
        if visitor is None:
            visitor = visitors.SetupLogger()
        rospy.logwarn(f"ln 374: calling expand_topic_name with ~/snapshots")
        self.default_snapshot_stream_topic_name = SnapshotStream.expand_topic_name(
            topic_name="~/snapshots"
        )

        ########################################
        # ROS Comms
        ########################################
        self.snapshot_stream_services = utilities.Services(
            service_details=[
                (
                    "close",
                    "~/snapshot_streams/close",
                    py_trees_srvs.CloseSnapshotStream,
                    self._close_snapshot_stream,
                ),
                (
                    "open",
                    "~/snapshot_streams/open",
                    py_trees_srvs.OpenSnapshotStream,
                    self._open_snapshot_stream,
                ),
                (
                    "reconfigure",
                    "~/snapshot_streams/reconfigure",
                    py_trees_srvs.ReconfigureSnapshotStream,
                    self._reconfigure_snapshot_stream,
                ),
            ],
            introspection_topic_name="snapshot_streams/services",
        )
        self.blackboard_exchange = blackboard.Exchange()
        self.blackboard_exchange.setup()

        ################################################################################
        # Parameters
        ################################################################################
        self.param_server = dynamic_reconfigure.server.Server(
            BehavioursConfig, self._set_parameters_callback
        )
        # TODO: Need to use timeout to set the parameter!
        # Get the resulting timeout
        # setup_timeout = self.node.get_parameter("setup_timeout").value
        setup_timeout = py_trees.common.Duration.INFINITE.value

        # Ugly workaround to accomodate use of the enum (TODO: rewind this)
        #   Need to pass the enum for now (instead of just a float) in case
        #   there are behaviours out in the wild that apply logic around the
        #   use of the enum
        if setup_timeout == py_trees.common.Duration.INFINITE.value:
            setup_timeout = py_trees.common.Duration.INFINITE

        ########################################
        # Behaviours
        ########################################
        try:
            super().setup(timeout=setup_timeout, visitor=visitor, **kwargs)
        except RuntimeError as e:
            if str(e) == "tree setup interrupted or timed out":
                raise exceptions.TimedOutError(str(e))
            else:
                raise

        ########################################
        # Setup Handlers
        ########################################
        # set a handler to publish future modifications whenever the tree is modified
        # (e.g. pruned). The tree_update_handler method is in the base class, set this
        # to the callback function here.
        self.tree_update_handler = self._on_tree_update_handler
        self.post_tick_handlers.append(self._snapshots_post_tick_handler)

        self.is_setup = True

    def _set_parameters_callback(
        self,
        param_config,
        level,
    ):
        """
        Callback that dynamically handles changes in parameters.

        Args:
            parameters: list of one or more declared parameters with updated values

        Returns:
            result of the set parameters requests
        """

        for parameter_name, parameter_value in param_config.items():
            if parameter_name == "default_snapshot_stream":
                rospy.loginfo(f"Got default_snapshot_stream = {parameter_value}")
                if self.default_snapshot_stream_topic_name in self.snapshot_streams:
                    self.snapshot_streams[
                        self.default_snapshot_stream_topic_name
                    ].shutdown()
                    if self.snapshot_streams[
                        self.default_snapshot_stream_topic_name
                    ].parameters.blackboard_activity:
                        self.blackboard_exchange.unregister_activity_stream_client()
                    del self.snapshot_streams[self.default_snapshot_stream_topic_name]
                if parameter_value:
                    rospy.loginfo("...resetting all SnapshotStream Parameters!")
                    try:
                        parameters = SnapshotStream.Parameters(
                            snapshot_period=param_config["default_snapshot_period"],
                            blackboard_data=param_config[
                                "default_snapshot_blackboard_data"
                            ],
                            blackboard_activity=param_config[
                                "default_snapshot_blackboard_activity"
                            ],
                        )
                    except Exception:
                        parameters = SnapshotStream.Parameters()
                    self.snapshot_streams[self.default_snapshot_stream_topic_name] = (
                        SnapshotStream(
                            topic_name=self.default_snapshot_stream_topic_name,
                            parameters=parameters,
                        )
                    )
                    if self.snapshot_streams[
                        self.default_snapshot_stream_topic_name
                    ].parameters.blackboard_activity:
                        self.blackboard_exchange.register_activity_stream_client()
            elif parameter_name == "default_snapshot_blackboard_data":
                if self.default_snapshot_stream_topic_name in self.snapshot_streams:
                    self.snapshot_streams[
                        self.default_snapshot_stream_topic_name
                    ].parameters.blackboard_data = parameter_value
            elif parameter_name == "default_snapshot_blackboard_activity":
                if self.default_snapshot_stream_topic_name in self.snapshot_streams:
                    if (
                        self.snapshot_streams[
                            self.default_snapshot_stream_topic_name
                        ].parameters.blackboard_activity
                        != parameter_value
                    ):
                        if parameter_value:
                            self.blackboard_exchange.register_activity_stream_client()
                        else:
                            self.blackboard_exchange.unregister_activity_stream_client()
                    self.snapshot_streams[
                        self.default_snapshot_stream_topic_name
                    ].parameters.blackboard_activity = parameter_value
            elif parameter_name == "default_snapshot_period":
                if self.default_snapshot_stream_topic_name in self.snapshot_streams:
                    self.snapshot_streams[
                        self.default_snapshot_stream_topic_name
                    ].parameters.snapshot_period = parameter_value
        return param_config

    def tick_tock(
        self,
        period_ms,
        number_of_iterations=py_trees.trees.CONTINUOUS_TICK_TOCK,
        pre_tick_handler=None,
        post_tick_handler=None,
    ):
        """
        Tick continuously at the period specified.

        This is a re-implementation of the
        :meth:`~py_trees.trees.BehaviourTree.tick_tock`
        tick_tock that takes advantage of the rospy timers so callbacks are interleaved inbetween
        ROS callbacks (keeps everything synchronous so no need for locks).

        Args:
            period_ms (:obj:`float`): sleep this much between ticks (milliseconds)
            number_of_iterations (:obj:`int`): number of iterations to tick-tock
            pre_tick_handler (:obj:`func`): function to execute before ticking
            post_tick_handler (:obj:`func`): function to execute after ticking
        """
        self.timer = rospy.timer.Timer(
            rospy.Duration(period_ms / 1000.0),  # unit 'seconds'
            functools.partial(
                self._tick_tock_timer_callback,
                number_of_iterations=number_of_iterations,
                pre_tick_handler=pre_tick_handler,
                post_tick_handler=post_tick_handler,
            ),
        )
        self.tick_tock_count = 0

    def shutdown(self):
        """
        Cleanly shut down timers and nodes.
        """
        # stop ticking if we're ticking
        if self.timer is not None:
            self.timer.shutdown()
        # call shutdown on each behaviour first, in case it has
        # some esoteric shutdown steps
        super().shutdown()

    # TODO: I think this is going to fail because a ROS1 timercallback is expected
    #   to take the rospy.TimerEvent event as the first argument.
    def _tick_tock_timer_callback(
        self, number_of_iterations, pre_tick_handler, post_tick_handler
    ):
        """
        Tick tock callback passed to the timer to be periodically triggered.

        Args:
            number_of_iterations (:obj:`int`): number of iterations to tick-tock
            pre_tick_handler (:obj:`func`): function to execute before ticking
            post_tick_handler (:obj:`func`): function to execute after ticking
        """
        if (
            number_of_iterations == py_trees.trees.CONTINUOUS_TICK_TOCK
            or self.tick_tock_count < number_of_iterations
        ):
            self.tick(pre_tick_handler, post_tick_handler)
            self.tick_tock_count += 1
        else:
            self.timer.shutdown()

    def _on_tree_update_handler(self):
        """
        Whenever there has been a modification to the tree (insertion/pruning), publish
        the snapshot.
        """
        # only worth notifying once we've actually commenced
        if self.statistics is not None:
            self.statistics.stamp = rospy.Time.now()
            for unused_topic_name, snapshot_stream in self.snapshot_streams.items():
                snapshot_stream.publish(
                    root=self.root,
                    changed=True,
                    statistics=self.statistics,
                    visited_behaviour_ids=self.snapshot_visitor.visited.keys(),
                    visited_blackboard_client_ids=self.snapshot_visitor.visited_blackboard_client_ids,
                )

    def _statistics_pre_tick_handler(self, tree: py_trees.trees.BehaviourTree):
        """
        Pre-tick handler that resets the statistics and starts the clock.

        Args:
            tree (:class:`~py_trees.trees.BehaviourTree`): the behaviour tree that has just been ticked
        """
        if len(self.time_series) == 10:
            self.time_series.popleft()
            self.tick_interval_series.popleft()

        start_time = rospy.Time.now()
        self.time_series.append(start_time.to_sec())
        if len(self.time_series) == 1:
            self.tick_interval_series.append(0.0)
        else:
            self.tick_interval_series.append(
                self.time_series[-1] - self.time_series[-2]
            )

        self.statistics = py_trees_msgs.Statistics()
        self.statistics.count = self.count
        self.statistics.stamp.data.secs = start_time.secs
        self.statistics.stamp.data.nsecs = start_time.nsecs
        self.statistics.tick_interval = self.tick_interval_series[-1]
        self.statistics.tick_interval_average = sum(self.tick_interval_series) / len(
            self.tick_interval_series
        )
        if len(self.tick_interval_series) > 1:
            self.statistics.tick_interval_variance = statistics.variance(
                self.tick_interval_series, self.statistics.tick_interval_average
            )
        else:
            self.statistics.tick_interval_variance = 0.0

    def _statistics_post_tick_handler(self, tree: py_trees.trees.BehaviourTree):
        """
        Post-tick handler that completes the statistics generation.

        Args:
            tree (:class:`~py_trees.trees.BehaviourTree`): the behaviour tree that has just been ticked
        """
        if self.statistics is None:
            rospy.logwarn("in _statistics_post_tick_handler, self.statistics is None")
            return

        curr_time = rospy.Time.now().to_sec()
        duration = curr_time - self.time_series[-1]

        if len(self.tick_duration_series) == 10:
            self.tick_duration_series.popleft()
        self.tick_duration_series.append(duration)

        self.statistics.tick_duration = duration
        self.statistics.tick_duration_average = sum(self.tick_duration_series) / len(
            self.tick_duration_series
        )
        if len(self.tick_duration_series) > 1:
            self.statistics.tick_duration_variance = statistics.variance(
                self.tick_duration_series, self.statistics.tick_duration_average
            )
        else:
            self.statistics.tick_duration_variance = 0.0

    def _snapshots_post_tick_handler(self, tree: py_trees.trees.BehaviourTree):
        """
        Post-tick handler that checks for changes in the tree/blackboard as a result
        of it's last tick and publish updates on ROS topics.

        Args:
            tree (:class:`~py_trees.trees.BehaviourTree`): the behaviour tree that has just been ticked
        """
        # checks
        if not self.is_setup:
            rospy.logerr("call setup() on this tree to initialise the ros components")
            return
        if self.root.tip() is None:
            rospy.logerr(
                "the root behaviour failed to return a tip [hint: tree is in an INVALID state, usually as a result of incorrectly coded behaviours]"
            )
            return

        # publish the default snapshot stream (useful for logging)
        if self.statistics is not None:
            for unused_topic_name, snapshot_stream in self.snapshot_streams.items():
                snapshot_stream.publish(
                    root=self.root,
                    changed=self.snapshot_visitor.changed,
                    statistics=self.statistics,
                    visited_behaviour_ids=self.snapshot_visitor.visited.keys(),
                    visited_blackboard_client_ids=self.snapshot_visitor.visited_blackboard_client_ids,
                )

        # every tick publish on watchers, clear activity stream (note: not expensive as watchers by default aren't connected)
        self.blackboard_exchange.post_tick_handler(
            visited_client_ids=self.snapshot_visitor.visited_blackboard_client_ids  # .keys()
        )

    def _close_snapshot_stream(
        self,
        request: py_trees_srvs.CloseSnapshotStreamRequest,  # noqa
    ) -> py_trees_srvs.CloseSnapshotStreamResponse:

        response = py_trees_srvs.CloseSnapshotStreamResponse()
        response.result = True
        try:
            self.snapshot_streams[request.topic_name].shutdown()
            if self.snapshot_streams[request.topic_name].parameters.blackboard_activity:
                self.blackboard_exchange.unregister_activity_stream_client()
            del self.snapshot_streams[request.topic_name]
        except KeyError:
            response.result = False
        return response

    def _open_snapshot_stream(
        self,
        request: py_trees_srvs.OpenSnapshotStreamRequest,  # noqa
    ) -> py_trees_srvs.OpenSnapshotStreamResponse:
        snapshot_stream = SnapshotStream(
            topic_name=request.topic_name,
            parameters=SnapshotStream.Parameters(
                blackboard_data=request.parameters.blackboard_data,
                blackboard_activity=request.parameters.blackboard_activity,
                snapshot_period=request.parameters.snapshot_period,
            ),
        )
        if snapshot_stream.parameters.blackboard_activity:
            self.blackboard_exchange.register_activity_stream_client()
        self.snapshot_streams[snapshot_stream.topic_name] = snapshot_stream

        response = py_trees_srvs.OpenSnapshotStreamResponse()
        response.topic_name = snapshot_stream.topic_name
        return response

    def _reconfigure_snapshot_stream(
        self,
        request: py_trees_srvs.ReconfigureSnapshotStreamRequest,  # noqa
    ) -> py_trees_srvs.ReconfigureSnapshotStreamResponse:
        response = py_trees_srvs.ReconfigureSnapshotStreamResponse()
        response.result = True
        try:
            snapshot_stream = self.snapshot_streams[request.topic_name]
            snapshot_stream.parameters.blackboard_data = (
                request.parameters.blackboard_data
            )
            if (
                snapshot_stream.parameters.blackboard_activity
                != request.parameters.blackboard_activity
            ):
                if request.parameters.blackboard_activity:
                    self.blackboard_exchange.register_activity_stream_client()
                else:
                    self.blackboard_exchange.unregister_activity_stream_client()
            snapshot_stream.parameters.blackboard_activity = (
                request.parameters.blackboard_activity
            )
            snapshot_stream.parameters.snapshot_period = (
                request.parameters.snapshot_period
            )
        except KeyError:
            response.result = False
        return response

    def _cleanup(self):
        with self.lock:
            # self.bag.close()
            self.interrupt_tick_tocking = True
            self._bag_closed = True


##############################################################################
# Tree Watcher
##############################################################################


class WatcherMode(enum.Enum):
    """An enumerator specifying the view mode for the watcher"""

    SNAPSHOTS = "SNAPSHOTS"
    """Render ascii/unicode snapshots from a snapshot stream"""
    DOT_GRAPH = "DOT_GRAPH"
    """Render with the dot graph representation of the static tree (using an application or text to console)."""


class Watcher(object):
    """
    The tree watcher sits on the other side of a running
    :class:`~py_trees_ros.trees.BehaviourTree` and is a useful mechanism for
    quick introspection of it's current state.

    Args:
        topic_name: location of the snapshot stream (optional)
        namespace_hint: refine search for snapshot stream services if there is more than one tree (optional)
        parameters: snapshot stream configuration controlling both on-the-fly stream creation and display
        statistics: display statistics

    .. seealso:: :mod:`py_trees_ros.programs.tree_watcher`
    """

    def __init__(
        self,
        topic_name: Optional[str] = None,
        namespace_hint: Optional[str] = None,
        parameters: Optional[SnapshotStream.Parameters] = None,
        statistics: bool = False,
        mode: WatcherMode = WatcherMode.SNAPSHOTS,
    ):
        self.topic_name = topic_name
        self.namespace_hint = namespace_hint

        self.mode = mode
        self.parameters: SnapshotStream.Parameters = (
            parameters if parameters is not None else SnapshotStream.Parameters()
        )
        self.statistics = statistics

        self.subscriber: Optional[rospy.Subscriber] = None
        self.snapshot_visitor = py_trees.visitors.SnapshotVisitor()
        self.done = False
        self.xdot_process = None
        self.rendered = None

        self.service_names: Dict[str, Optional[str]] = {"open": None, "close": None}
        self.service_type_strings = {
            "open": "py_trees_ros_interfaces.srv.OpenSnapshotStream",
            "close": "py_trees_ros_interfaces.srv.CloseSnapshotStream",
        }
        self.service_types = {
            "open": py_trees_srvs.OpenSnapshotStream,
            "close": py_trees_srvs.CloseSnapshotStream,
        }

    def setup(self, timeout_sec: float):
        """
        Creates the node and checks that all of the server-side services are available
        for calling on to open a connection.

        Args:
            timeout_sec: time (s) to wait (use common.Duration.INFINITE to block indefinitely)

        Raises:
            :class:`~py_trees_ros.exceptions.NotFoundError`: if services/topics were not found
            :class:`~py_trees_ros.exceptions.MultipleFoundError`: if multiple services were found
            :class:`~py_trees_ros.exceptions.TimedOutError`: if service clients time out waiting for the server
        """
        # open a snapshot stream
        self.services: Dict[str, rospy.ServiceProxy] = {}
        if self.topic_name is None:
            # discover actual service names
            for service_name in self.service_names.keys():
                # can raise NotFoundError and MultipleFoundError
                self.service_names[service_name] = utilities.find_service(
                    service_type=self.service_type_strings[service_name],
                    namespace=self.namespace_hint,
                    timeout=timeout_sec,
                )
            # create service clients
            self.services["open"] = self.create_service_client(key="open")
            self.services["close"] = self.create_service_client(key="close")
            # request a stream
            request = self.service_types["open"]._request_class()
            request.parameters.blackboard_data = self.parameters.blackboard_data
            request.parameters.blackboard_activity = self.parameters.blackboard_activity
            request.parameters.snapshot_period = self.parameters.snapshot_period
            response = self.services["open"](request)
            self.topic_name = response.topic_name
        # connect to a snapshot stream
        start_time = time.monotonic()
        while True:
            # if self.node.count_publishers(self.topic_name) > 0:
            #     break
            all_topics = [topic for topic, _msg_type in rospy.get_published_topics()]
            if self.topic_name in all_topics:
                break
            elapsed_time = time.monotonic() - start_time
            if elapsed_time > timeout_sec:
                raise exceptions.TimedOutError(
                    "timed out waiting for a snapshot stream publisher [{}]".format(
                        self.topic_name
                    )
                )
            time.sleep(0.1)
        self.subscriber = rospy.Subscriber(
            self.topic_name,
            py_trees_msgs.BehaviourTree,
            self.callback_snapshot,
        )

    def shutdown(self):
        if "close" in self.services:
            request = self.service_types["close"].Request()
            request.topic_name = self.topic_name
            _unused_response = self.service_types["close"](request)

    def create_service_client(self, key: str) -> rospy.ServiceProxy:
        """
        Convenience api for opening a service client and waiting for the service to appear.

        Args:
            key: one of 'open', 'close'.

        Raises:
            :class:`~py_trees_ros.exceptions.NotReadyError`: if setup() wasn't called to identify the relevant services to connect to.
            :class:`~py_trees_ros.exceptions.TimedOutError`: if it times out waiting for the server
        """
        if self.service_names[key] is None:
            raise exceptions.NotReadyError(
                "no known '{}' service known [did you call setup()?]".format(
                    self.service_types[key]
                )
            )
        client = rospy.ServiceProxy(
            self.service_names[key],
            self.service_types[key],
        )
        # hardcoding timeouts will get us into trouble
        timeout_sec = 3.0
        try:
            client.wait_for_service(timeout_sec)
            return client
        except rospy.exceptions.ROSException:
            raise exceptions.TimedOutError(
                "timed out waiting for {}".format(self.service_names["close"])
            )

    def callback_snapshot(self, msg):
        """
        Formats the string message coming in.

        Args
            msg (:class:`py_trees_ros_interfaces.msg.BehaviourTree`):serialised snapshot
        """
        ####################
        # Processing
        ####################
        self.snapshot_visitor.previously_visited = self.snapshot_visitor.visited
        self.snapshot_visitor.visited = {}
        serialised_behaviours = {}
        root_id = None
        for serialised_behaviour in msg.behaviours:
            if serialised_behaviour.parent_id == uuid_msgs.msg.UniqueID():
                root_id = conversions.msg_to_uuid4(serialised_behaviour.own_id)
            serialised_behaviours[
                conversions.msg_to_uuid4(serialised_behaviour.own_id)
            ] = serialised_behaviour

        def deserialise_tree_recursively(msg):
            behaviour = conversions.msg_to_behaviour(msg)
            for serialised_child_id in msg.child_ids:
                child_id = conversions.msg_to_uuid4(serialised_child_id)
                child = deserialise_tree_recursively(serialised_behaviours[child_id])
                # invasive hack to revert the dummy child we added in msg_to_behaviour
                if isinstance(behaviour, py_trees.decorators.Decorator):
                    behaviour.children = [child]
                    behaviour.decorated = behaviour.children[0]
                else:
                    behaviour.children.append(child)
                child.parent = behaviour
            # set the current child so tip() works properly everywhere
            if behaviour.children:
                if msg.current_child_id != uuid_msgs.msg.UniqueID():
                    current_child_id = conversions.msg_to_uuid4(msg.current_child_id)
                    for index, child in enumerate(behaviour.children):
                        if child.id == current_child_id:
                            # somewhat ugly not having a consistent api here
                            if isinstance(behaviour, py_trees.composites.Selector):
                                behaviour.current_child = child
                            elif isinstance(behaviour, py_trees.composites.Sequence):
                                behaviour.current_index = index
                            # else Parallel, nothing to do since it infers
                            # the current child from children's status on the fly
                            break
            if msg.is_active:
                self.snapshot_visitor.visited[behaviour.id] = behaviour.status
            return behaviour

        # we didn't set the tip in any behaviour, but nothing depends
        # on that right now
        root = deserialise_tree_recursively(serialised_behaviours[root_id])

        ####################
        # Streaming
        ####################
        if self.mode == WatcherMode.SNAPSHOTS:
            if msg.changed:
                colour = console.green
            else:
                colour = console.bold
            ####################
            # Banner
            ####################
            title = "Tick {}".format(msg.statistics.count)
            print(colour + "\n" + 80 * "*" + console.reset)
            print(colour + "* " + console.bold + title.center(80) + console.reset)
            print(colour + 80 * "*" + "\n" + console.reset)
            ####################
            # Tree Snapshot
            ####################
            print(
                py_trees.display.unicode_tree(
                    root=root,
                    visited=self.snapshot_visitor.visited,
                    previously_visited=self.snapshot_visitor.previously_visited,
                )
            )
            print(colour + "-" * 80 + console.reset)
            ####################
            # Stream Variables
            ####################
            if self.parameters.blackboard_data:
                print("")
                print(colour + "Blackboard Data" + console.reset)
                indent = " " * 4
                if not msg.blackboard_on_visited_path:
                    print(
                        console.cyan
                        + indent
                        + "-"
                        + console.reset
                        + " : "
                        + console.yellow
                        + "-"
                        + console.reset
                    )
                else:
                    # could probably re-use the unicode_blackboard by passing a dict to it
                    # like we've done for the activity stream
                    max_length = 0
                    for variable in msg.blackboard_on_visited_path:
                        max_length = (
                            len(variable.key)
                            if len(variable.key) > max_length
                            else max_length
                        )
                    for variable in msg.blackboard_on_visited_path:
                        print(
                            console.cyan
                            + indent
                            + "{0: <{1}}".format(variable.key, max_length + 1)
                            + console.reset
                            + ": "
                            + console.yellow
                            + "{0}".format(variable.value)
                            + console.reset
                        )
            ####################
            # Stream Activity
            ####################
            if self.parameters.blackboard_activity:
                print("")
                print(colour + "Blackboard Activity Stream" + console.reset)
                if msg.blackboard_activity:
                    print(
                        py_trees.display.unicode_blackboard_activity_stream(
                            msg.blackboard_activity, indent=0, show_title=False
                        )
                    )
                else:
                    indent = " " * 4
                    print(
                        console.cyan
                        + indent
                        + "-"
                        + console.reset
                        + " : "
                        + console.yellow
                        + "-"
                        + console.reset
                    )
            ####################
            # Stream Statistics
            ####################
            if self.statistics:
                print("")
                print(colour + "Statistics" + console.reset)
                print(
                    console.cyan
                    + "    Timestamp: "
                    + console.yellow
                    + "{}".format(msg.statistics.stamp.data.to_sec())
                )
                print(
                    console.cyan
                    + "    Duration : "
                    + console.yellow
                    + "{:.3f}/{:.3f}/{:.3f} (ms) [time/avg/stddev]".format(
                        msg.statistics.tick_duration * 1000,
                        msg.statistics.tick_duration_average * 1000,
                        math.sqrt(msg.statistics.tick_duration_variance) * 1000,
                    )
                )
                print(
                    console.cyan
                    + "    Interval : "
                    + console.yellow
                    + "{:.3f}/{:.3f}/{:.3f} (s) [time/avg/stddev]".format(
                        msg.statistics.tick_interval,
                        msg.statistics.tick_interval_average,
                        math.sqrt(msg.statistics.tick_interval_variance),
                    )
                )
                print(console.reset)

        ####################
        # Dot Graph
        ####################
        elif self.mode == WatcherMode.DOT_GRAPH and not self.rendered:
            self.rendered = True
            directory_name = tempfile.mkdtemp()
            py_trees.display.render_dot_tree(
                root=root,
                target_directory=directory_name,
                with_blackboard_variables=self.parameters.blackboard_data,
            )
            xdot_program = py_trees.utilities.which("xdot")

            if not xdot_program:
                print("")
                console.logerror("No xdot viewer found [hint: sudo apt install xdot]")
                print("")
                print(py_trees.display.dot_tree(root=root).to_string())
                self.done = True
                self.xdot_process = None
                return

            filename = py_trees.utilities.get_valid_filename(root.name) + ".dot"
            if xdot_program:
                try:
                    self.xdot_process = subprocess.Popen(
                        [xdot_program, os.path.join(directory_name, filename)]
                    )
                except KeyboardInterrupt:
                    pass
            self.done = True
