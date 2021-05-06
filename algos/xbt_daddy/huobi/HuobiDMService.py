#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 20180917
# @Author  : zhaobo
# @github  : 

from .HuobiDMUtil import http_get_request, api_key_post, api_key_get

class HuobiDM:

    def __init__(self,url,access_key,secret_key):
        self.__url = url
        self.__access_key = access_key
        self.__secret_key = secret_key

    def send_get_request(self, request_path, params):
        return api_key_get(self.__url, request_path, params, self.__access_key, self.__secret_key)

    def send_post_request(self, request_path, params):
        return api_key_post(self.__url, request_path, params, self.__access_key, self.__secret_key)