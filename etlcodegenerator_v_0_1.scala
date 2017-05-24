# -*- coding: utf-8 -*-
"""
Created on Tue May 16 14:23:39 2017

@author: eib676
"""

import json
from collections import OrderedDict

# json_etl = """{
#      "1": ["readFile", "read", "readoutDF"],
#      "2": ["readoutDF", "filter", "sortOutDF"],
#      "3": ["sortOutDF", "sort", "deDupoutDF"],
#      "4": ["deDupoutDF", "groupBy", "groupByoutDF"],
#      "5": ["groupByoutDF", "write", "WriteFile2"]
#    }"""

#json_etl = open("C:\Users\eib676\Documents\FS tech Innv\etlJobIMDB.json").read().decode("UTF-8")
#json_config = open("C:\Users\eib676\Documents\FS tech Innv\etlConfIMDB.json").read().decode("UTF-8")
json_etl = open("C:\Users\eib676\Documents\FS tech Innv\etlJobIDQ.json").read().decode("UTF-8")
json_config = open("C:\Users\eib676\Documents\FS tech Innv\etlConfIDQ.json").read().decode("UTF-8")
scala_template = "C:\Users\eib676\NeonWorkspace\etlsparkscala\src\main\scala\org\capone\etlsparkscala\SparkScalaTemplate.scala"
pgm_name = "IDQDelta"
output_file = "C:\Users\eib676\NeonWorkspace\etlsparkscala\src\main\scala\org\capone\etlsparkscala\\" + pgm_name + ".scala"

etlConfig = json.loads(json_config, object_pairs_hook=OrderedDict)
etlFlow = json.loads(json_etl, object_pairs_hook=OrderedDict)


def construct_map_argument(etl_flow_key):
    config_str = ",".join(['"' + k + '"' + " -> " + '"' + v + '"'
                           for k, v in etlConfig[etl_flow_key].iteritems()])
    return config_str


def construct_dedup_map_argument(etl_flow_key):
    config_str_list = []
    for k, v in etlConfig[etl_flow_key].iteritems():
        if k == "deDupCols":
            config_str_list.append('"' + k + '"' + " -> " + '"' + ",".join(v) + '"')
        else:
            config_str_list.append('"' + k + '"' + " -> " + '"' + v + '"')
    return ",".join(config_str_list)


def construct_filter_argument(etl_flow_key):
    config_str_list = []
    for k, v in etlConfig[etl_flow_key].iteritems():
        if k == "filterCond":
            config_str_list.append('"' + k + '"' + " -> " + '"""' + v + '"""')
        else:
            config_str_list.append('"' + k + '"' + " -> " + '"' + v + '"')

    return ",".join(config_str_list)


def construct_argument_for_string(etl_flow_key, cond_key):
    print("test1 ---->", etl_flow_key)
    print("test2 ---->", cond_key)
    config_str = '"' + etlConfig[etl_flow_key][cond_key] + '"'
    return config_str


def construct_argument_for_list_string(etl_flow_key, cond_key):
    print(cond_key)
    print("test ---->", etlConfig)
    config_str = "List(" + ",".join(['"' + col + '"'
                                     for col in etlConfig[etl_flow_key][cond_key]]) + ")"
    print("imout",config_str)
    
    return config_str


def construct_argument_for_rfmt_cond_expr_string(etl_flow_key, cond_key):
    config_str_list = []
    print(etlConfig[etl_flow_key][cond_key])
    if len(etlConfig[etl_flow_key][cond_key]) == 1:
        rfunc = etlConfig[etl_flow_key][cond_key]
        print("rfunc -->", rfunc)
        if (rfunc[0][0] == "" and rfunc[0][1] == ""):
            config_str = 'Nil'
        else:
            for rfunc in etlConfig[etl_flow_key][cond_key]:
                print(rfunc)
                config_str_list.append('("""' + rfunc[0] + '"""' + ',' + '"' + rfunc[1] + '")')
            config_str = "List(" + ",".join(config_str_list) + ")"
    else:
        for rfunc in etlConfig[etl_flow_key][cond_key]:
            print(rfunc)
            config_str_list.append('("""' + rfunc[0] + '"""' + ',' + '"' + rfunc[1] + '")')
        config_str = "List(" + ",".join(config_str_list) + ")"
    return config_str


def construct_argument_for_tuple_type(etl_flow_key, cond_key):
    config_str_list = []
    for gfunc in etlConfig[etl_flow_key][cond_key]:
        config_str_list.append("(" + ",".join(['"' + gc + '"' for gc in gfunc]) + ")")
    config_str = "List(" + ",".join(config_str_list) + ")"
    return config_str


def construct_elt_flow(in_df, etl_f, out_df):
    def read_statement(input_df, etl_func, output_df):
        return ("  val " + output_df + " = " + etl_func +
                "DataFrame(spark," + "Map(" + construct_map_argument(input_df) + "))")

    def write_statement(input_df, etl_func, output_df):
        return ("  val " + output_df + " = " + etl_func +
                "DataFrame(" + input_df + ", " + "Map(" + construct_map_argument(input_df) + "))")

    def filter_statement(input_df, etl_func, output_df):
        if isinstance(output_df, basestring):
            config_str = ("  val " + output_df + "List = " + etl_func +
                    "DataFrame(" + input_df + ", " +
                    "Map(" + construct_filter_argument(input_df) + "))"
                    + "\n" + "  val " + output_df + " = " + output_df + "List" + "(0)")
            
        else:
            config_str = ("  val " + output_df[0] + output_df[1] + " = " + etl_func + "DataFrame(" + input_df + ", " +
                    "Map(" + construct_filter_argument(input_df) + "))" + "\n" + "  val " +
                    output_df[0] + " = " + output_df[0] + output_df[1] + "(0)" + "\n" +
                    "  val " + output_df[1] + " = " + output_df[0] + output_df[1] + "(1)")
        return config_str

    def deDup_statement(input_df, etl_func, output_df):
        if isinstance(output_df, basestring):
            config_str = ("  val " + output_df + "List = " + etl_func +
                    "DataFrame(" + input_df + ", " + "Map(" + construct_dedup_map_argument(input_df) + "))" + "\n"
                    + "  val " + output_df + " = " + output_df + "List" + "(0)")

        else:
            config_str = ("  val " + output_df[0] + output_df[1] + " = " + etl_func +
                    "DataFrame(" + input_df + ", " + "Map(" + construct_dedup_map_argument(input_df) + "))"
                    + "\n" + "  val " + output_df[0] + " = " + output_df[0] + output_df[1] + "(0)" + "\n" +
                    "  val " + output_df[1] + " = " + output_df[0] + output_df[1] + "(1)")
        return config_str

    def groupBy_statement(input_df, etl_func, output_df):
        config_str = ("  val " + output_df + " = " + etl_func + "DataFrame(" + input_df + ", " +
                construct_argument_for_list_string(input_df, "groupByCols") + ", "
                + "\n" + "        " + construct_argument_for_tuple_type(input_df, "aggCond")
                + ")")
        return config_str

    def reformat_statement(input_df, etl_func, output_df):
        config_str = ("  val " + output_df + " = " + etl_func + "DataFrame(" + input_df + ", " +
                construct_argument_for_rfmt_cond_expr_string(input_df, "reformatCond") + ", " + "\n" +
                "        " + construct_argument_for_list_string(input_df, "selectDropCols") + ")")
        return config_str
        
    def sort_statement(input_df, etl_func, output_df):
        config_str = ("  val " + output_df + " = " + etl_func + "DataFrame(" + input_df + ", " +
                construct_argument_for_tuple_type(input_df, "sortCond") + ")")
        return config_str

    def join_statement(input_df, etl_func, output_df):
        print("inputdf-->", input_df)
        if isinstance(output_df, basestring):
            output_df_temp = [output_df] + ["_"] * 5
        else:
            output_df_temp = output_df + ["_"] * 5
        ouput_final = output_df_temp[:5]
        config_str = ("  val " + "(" + ",".join(ouput_final) +
        ")" + " = " + etl_func + "DataFrame(spark" + "," + input_df[0] + ", " + input_df[1] + ", " +
        construct_argument_for_list_string(input_df[0], "keys") + "," +
        construct_argument_for_list_string(input_df[1], "keys") + "," +
        construct_argument_for_string(input_df[0], "joinType") + "," +
        "List(" + ",".join(['"' + x + '"' for x in ouput_final]) + "))")
        return config_str
        

    def diff_statement(input_df, etl_func, output_df):
        if isinstance(output_df, basestring):
            output_df_temp = [output_df] + ["_"] * 3
        else:
            output_df_temp = output_df + ["_"] * 3
        ouput_final = output_df_temp[:3]
        return "  val " + "(" + ",".join(ouput_final) + ")" + " = " + etl_func + "DataFrame(" + ",".join(input_df) + ")"

    def union_statement(input_df, etl_func, output_df):
        return "  val " +  output_df + " = " + etl_func + "DataFrame(" + ",".join(input_df) + ")"

    def replicate_statement(input_df, etl_func, output_df):
        return "  val " + "(" + ",".join(output_df) + ")" + " = " + "(" + ",".join([input_df] * 2) + ")"

    return eval(etl_f + '_statement' + '(in_df, etl_f, out_df)')


with open(scala_template, 'r') as f:
    read_data = f.read()
f.closed

write_data = read_data \
    .replace("1234567890", "\n".join([construct_elt_flow(x[0], x[1], x[2])
                                      for x in etlFlow.values()])) \
    .replace("SparkScalaTemplate", pgm_name)

print(write_data)
f = open(output_file, 'w')
f.write(write_data)
