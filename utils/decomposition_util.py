import os
import sys
sys.path.insert(0, '../')
import re
import json
import logging
logging.basicConfig(level=logging.WARNING)


def remove_meta(directory):
    """
    Removing the first 3 and last 4 lines of the STDOUT in the txt files
    :param directory:
    :return:
    """
    for filename in os.listdir(directory):
        if filename.endswith(".txt"):
            with open(directory + filename, 'r') as fin:
                data = fin.read().splitlines(True)
            with open(directory + filename, 'w') as fout:
                fout.writelines(data[3:-4])

def to_dict(file, write=False):
    with open(file) as f:
        decomp_file = f.read().splitlines()

    newlines = ["[ "]
    in_root = in_excl = in_group = in_join = in_union = in_leftjoin = in_proj = False
    excl_level = group_level = join_level = union_level = leftjoin_level = proj_level = 0


    for line in decomp_file:
        if "QueryRoot" in line:
            in_root = True
            continue
        elif "Projection" in line and not "Elem" in line:
            continue
        elif "LocalVars" in line:
            continue
        elif not in_root:
            continue

        line = line.replace('"', "'")
        level = int((len(line) - len(line.lstrip())) / 3)
        if in_proj and level <= proj_level:
            in_proj = False
            newlines[-1] = newlines[-1].replace(", ", ']}, \n')
        if in_excl and level <= excl_level:
            in_excl = False
            if in_group:
                newlines[-1] = newlines[-1].replace("\n", '" }, \n')
            elif in_join:
                newlines[-1] = newlines[-1].replace("\n", '" }, \n')
            elif in_leftjoin:
                newlines[-1] = newlines[-1].replace("\n", '" }, \n')
            elif in_union:
                newlines[-1] = newlines[-1].replace("\n", '" }, \n')
            else:
                newlines[-1] = newlines[-1].replace("\n", '" }, \n')

        if in_group and level <= group_level:
            in_group = False
            if in_join:
                newlines[-1] = newlines[-1].replace(", \n", ']}, \n')
            elif in_leftjoin:
                newlines[-1] = newlines[-1].replace("\n", '" }, \n')
            elif in_union:
                newlines[-1] = newlines[-1].replace(", \n", ']}, \n')
            else:
                newlines[-1] = newlines[-1].replace(", \n", ']} \n')

        if in_union and level <= union_level:
            in_union = False
            newlines[-1] = newlines[-1].replace(", \n", ']} \n')

        if in_join and level <= join_level:
            in_join = False
            if in_leftjoin:
                newlines[-1] = newlines[-1].replace(", \n", ']}, \n')
            else:
                newlines[-1] = newlines[-1].replace(", \n", ']} \n')

        if in_leftjoin and level <= leftjoin_level:
            in_leftjoin = False
            newlines[-1] = newlines[-1].replace(", \n", ']} \n')

        if "ProjectionElemList" in line:
            in_proj = True
            proj_level = level
            nl = line.replace("ProjectionElemList", '{ "ProjectionElemList" : [\n')
            newlines.append(nl)
        elif "ExclusiveStatement" in line or \
                "StatementSourcePattern" in line or \
                "EmptyStatementPattern" in line:
            in_excl = True
            excl_level = level
            if "ExclusiveStatement" in line:
                nl = line.replace("ExclusiveStatement", '{ "ExclusiveStatement" : "\n')
            if "StatementSourcePattern" in line:
                nl = line.replace("StatementSourcePattern", '{ "StatementSourcePattern" : "\n')
            if "EmptyStatementPattern" in line:
                nl = line.replace("EmptyStatementPattern", '{ "EmptyStatementPattern" : "\n')
            newlines.append(nl)
        elif "ExclusiveGroup" in line:
            in_group = True
            group_level = level
            nl = line.replace("ExclusiveGroup", '{ "ExclusiveGroup" : [ \n')
            newlines.append(nl)
        elif "NJoin" in line:
            in_join = True
            join_level = level
            nl = line.replace("NJoin", '{ "NJoin" : [ \n')
            newlines.append(nl)
        elif "LeftJoin" in line:
            in_leftjoin = True
            leftjoin_level = level
            nl = line.replace("LeftJoin", '{ "LeftJoin" : [ \n')
            newlines.append(nl)
        elif "NUnion" in line or "Union" in line:
            in_union = True
            union_level = level
            if "NUnion" in line:
                nl = line.replace("NUnion", '{ "NUnion" : [ \n')
            else:
                nl = line.replace("Union", '{ "Union" : [ \n')
            newlines.append(nl)
        elif "ProjectionElem" in line:
            newlines.append('"' + re.findall(r".*'(.*)'.*", line)[0] + '", ')
        else:
            newlines.append(line + " \n")
    if in_excl:
        newlines[-1] = newlines[-1].replace("\n", '"} \n')
    if in_join:
        newlines[-1] = newlines[-1].replace("\n", ']} \n')
    if in_group:
        newlines[-1] = newlines[-1].replace("\n", ']} \n')
    if in_leftjoin:
        newlines[-1] = newlines[-1].replace("\n", ']} \n')
    if in_union:
        newlines[-1] = newlines[-1].replace("\n", ']} \n')
    newlines.append("]")
    st = "".join(newlines).replace("\n", "")
    try:
        jd = json.loads(st)
    except Exception as e:
        print(file)
        print(e)

    if write:
        with open(file.split(".")[0] + ".json", 'w') as out_file:
            try:
                json.dump(jd, out_file)
            except Exception as e:
                out_file.write(st)
    return jd


