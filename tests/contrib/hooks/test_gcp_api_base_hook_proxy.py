# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import unittest
import airflow
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None


class TestGcpApiBaseHook(unittest.TestCase):

    def setUp(self):
        airflow.configuration.load_test_config()
        self.hook = GoogleCloudBaseHook()

    def side_effect_get_proxyconfig(self):
        """
        Side effect to mock proxy details
        """
        mock_proxy_details = {'proxy_host': 'abc.com', 'proxy_port': 8080, 'proxy_type': 'HTTP_NO_TUNNEL'}
        return mock_proxy_details

    @mock.patch('airflow.contrib.hooks.gcp_api_base_hook.GoogleCloudBaseHook._get_credentials')
    @mock.patch('airflow.contrib.hooks.gcp_api_base_hook.GoogleCloudBaseHook._get_proxy_obj')
    def test_authorize_no_proxy_object(self, mock_get_proxy_obj, mock_get_credentials):
        """
        Test the creation of authed_http object when proxy object returned is None
        """
        mock_get_proxy_obj.return_value = None
        mock_get_credentials.return_value = None
        self.assertIsNotNone(self.hook._authorize())

    @mock.patch('airflow.contrib.hooks.gcp_api_base_hook.GoogleCloudBaseHook._get_credentials')
    @mock.patch('airflow.contrib.hooks.gcp_api_base_hook.GoogleCloudBaseHook._get_useproxy')
    @mock.patch('airflow.hooks.base_hook.BaseHook.get_proxyconfig')
    def test_authorize_with_proxy_object(self,mock_get_proxy_obj,mock_useproxy_true, mock_get_credentials):
        """
        Test the creation of authed_http object when use_proxy is True
        """
        mock_useproxy_true.return_value = True
        mock_get_proxy_obj.side_effect = self.side_effect_get_proxyconfig
        mock_get_credentials.return_value = None
        self.assertIsNotNone(self.hook._authorize())

    @mock.patch('airflow.contrib.hooks.gcp_api_base_hook.GoogleCloudBaseHook._get_useproxy')
    @mock.patch('airflow.hooks.base_hook.BaseHook.get_proxyconfig')
    def test_get_proxy_obj_useproxy_true(self, mock_get_proxy_config, mock_useproxy_true):
        """
        Test the creation of proxy object when use_proxy is True
        """
        mock_useproxy_true.return_value = True
        mock_get_proxy_config.side_effect = self.side_effect_get_proxyconfig
        proxy_obj = self.hook._get_proxy_obj()
        self.assertIsNotNone(proxy_obj)

    @mock.patch('airflow.contrib.hooks.gcp_api_base_hook.GoogleCloudBaseHook._get_useproxy')
    @mock.patch('airflow.hooks.base_hook.BaseHook.get_proxyconfig')
    def test_get_proxy_obj_useproxy_false(self, mock_get_proxy_config,mock_useproxy_false):
        """
        Test the scenario when use_proxy is False
        """
        mock_useproxy_false.return_value = False
        mock_get_proxy_config.side_effect = self.side_effect_get_proxyconfig
        proxy_obj = self.hook._get_proxy_obj()
        self.assertIsNone(proxy_obj)

    @mock.patch('airflow.configuration.conf.getboolean')
    def test_get_useproxy(self, mock_getboolean_true):
        """
        Test use_proxy method when use_proxy configuration is True
        """
        mock_getboolean_true.return_value = True
        self.assertEqual(self.hook._get_useproxy(), True)

    @mock.patch('airflow.configuration.conf.getboolean')
    def test_get_useproxy_exception(self, mock_getboolean):
        """
        Test use_proxy method when use_proxy section is absent in Airflow configuration
        """
        mock_getboolean.side_effect = airflow.exceptions.AirflowConfigException
        self.assertEqual(self.hook._get_useproxy(), False)

    def test_get_proxy_type(self):
        """
        Test get_proxy_type mapping when proxy type matches
        """
        proxy_obj = {'proxy_type': "HTTP"}
        self.assertIsNotNone(self.hook._get_proxy_type(proxy_obj))

    def test_get_proxy_type_invalid(self):
        """
        Test get_proxy_type with invalid proxy type
        """
        proxy_obj = {'proxy_type': "Invalid_type"}
        self.assertIsNone(self.hook._get_proxy_type(proxy_obj))




