#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   Description :	TODO：
   SourceFile  :	Esbase
   Author      :	zimo
   Date	       :	2022/12/6
-------------------------------------------------
"""
__coding__ = "utf-8"
__author__ = "itcast team"

from dataclasses import dataclass

# 自动装配__init__和__str__
@dataclass
class ESMeta:
    inType: str
    esNodes: str
    esIndex: str
    esType: str
    selectFields: str


    def fromDictToEsMeta(ruleDict: dict):
        return ESMeta(ruleDict["inType"],
                      ruleDict["esNodes"],
                      ruleDict["esIndex"],
                      ruleDict["esType"],
                      ruleDict["selectFields"])

    def fromStrToEsMeta(ruleStr: str):
        ruleDict = {}
        fourRuleList = ruleStr.split('##')
        for item in fourRuleList:
            ruleDict[item.split('=')[0]] = item.split('=')[1]
        return ESMeta(ruleDict["inType"],
                      ruleDict["esNodes"],
                      ruleDict["esIndex"],
                      ruleDict["esType"],
                      ruleDict["selectFields"])