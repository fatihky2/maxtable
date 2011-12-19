import simplejson
import sys

def decode_member(item, i):
    if item['type'] == "string":
        ret_str = "        %s = mt_cli_get_colvalue(exec_ctx, resp, %s, &collen);\n" % (item['name'], i)
    else:
        ret_str = "        %s = *((int *)mt_cli_get_colvalue(exec_ctx, resp, %s, &collen));\n" % (item['name'], i)

    return ret_str

def encode_member(item):
    if item['type'] == "string":
        ret_str = "            << %s.c_str() << \", \"\n" % item['name']
    else:
        ret_str = "            << (int)%s << \", \"\n" % item['name']
    return ret_str

def schema_member(item, key_name):
    if item['type'] == "string" : sql_type = "varchar"
    else : sql_type = "int"
    ret_str = "%s %s, " % (item['name'], sql_type)
    return ret_str

def idl_2cpp(value):
    if value.has_key('desc'):
        print "/* %s */" % value['desc']
        
    key_name = value['key']
        
    print "%s %s" %(value['type'], value['name'])
    print "{"
        
    decode = ""
    encode = ""
    schema = "%s varchar, " % key_name
        
    i = 0
    for item in value['members']:
        if item.has_key('desc'):
            print "    /* %s */" % item['desc']
        print "    %s    %s;" % (item['type'], item['name'])
            
        decode += decode_member(item, i)
        encode += encode_member(item)
        if item['name'] != key_name:
            schema += schema_member(item, key_name)
        i += 1
    
    schema = schema[:-2]
    encode = encode[:-9]
            
    print "    static string genCreateSql(const string &tableName)"
    print "    {"
    print "        ostringstream oss;"
    print "        oss << \"create table \" << tableName << \"(%s)\";" % schema
    print "        return oss.str();"
    print "    }"
    
    print "    string genInsertSql(const string &tableName)"
    print "    {"
    print "        ostringstream oss;"
    print "        oss << \"insert into \" << tableName << \"(\""
    #print "            << %s << \", \"\n" % item['name']
    print encode
    print "            << \")\";"
    print "        return oss.str();"
    print "    }"

    print "    void getSelectValue(MT_CLI_EXEC_CONTEX *exec_ctx, char *resp)"
    print "    {"
    print "        int collen;"
    print decode
    print "    }"
    
    print "};"

if __name__ == '__main__':
    idl_fname = sys.argv[1]
    idl_fp = open(idl_fname)
    
    json_table = simplejson.load(idl_fp)
    
    cpp_head = "__IDL_%s_H__" % json_table['headname'].upper()
    print "#ifndef %s" % cpp_head
    print "#define %s\n" % cpp_head
    print "#include <string>"
    print "#include <sstreams>"
    print "extern \"C\"{"
    print "#include \"interface.h\""
    print "}\n"
        
    if json_table.has_key('include'):
        pass
        
    print "using namespace std;\n"
    print "namespace objclientidl"
    print "{"
    
    for i in json_table['items']:
        idl_2cpp(i)
    
    print "}\n#endif"

