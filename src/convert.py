# -*- coding: utf-8 -*-

import os
import sys

TypeFileIn = 'TypeDefinition.json'
TypeFileOut = 'TypeDefinition.java'

def convert():

    with open(TypeFileIn, 'r') as f:
        type_file_in = f.read()

    lines = type_file_in.split('\n')
    type_file_out = 'public class TypeDefiniton {\n' \
					'\tpublic static String TypeFile = ""\n'
    for l in lines:
    	type_file_out = type_file_out + '\t\t+ "' + l.replace('"', '\\"') + '"\n'

    type_file_out = type_file_out + '\t;\n}\n'

    with open(TypeFileOut, 'wb+') as f:
    	f.write(type_file_out)

convert()