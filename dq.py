import pandas as pd

import re
def check_datatype(value,exp_datatype):
    if (type(value)==exp_datatype):
        return True
    return False

def check_nullability(value):
    if (value is None):
        return False
    return True

def check_custom_sql(value,validation_set):
    if (value in validation_set):
        return True
    return False

def check_regex(value,pattern):
    pat = re.compile(pattern)
    if pat.match(value):
        return True
    return False

try:
    tables=curs.execute("select distinct stg_table_name from dq_rules_config").fetchall()
    for table in tables:
        columns = curs.execute("select distinct stg_column_name from dq_rules_config where stg_table_name='"+table[0]+"'").fetchall()
        columns = [column[0] for column in columns]
  
        rules=[]
        for column in columns:

            rules.append(curs.execute("select dq_rule,rule_desc from dq_rules_config where stg_table_name='"+table[0]+"' and stg_column_name = '"+column+"'").fetchall())

        columns.append('row_id')
        for row in curs.execute('select '+','.join(columns)+' from '+table[0]):
            global_correct_record=True
            local_correct_record=True
            error_column={}

#             print(row)
            for i in range(len(columns)-1):
                error_reason=[]
                for rule in rules[i]:
                    if(rule[0]=='NotNull'):


                        if(rule[1]=='true'):
                            local_correct_record=check_nullability(row[i])
                            if global_correct_record==True:
                                global_correct_record=local_correct_record

                            if not local_correct_record:
                                error_reason.append('NotNull')
                                error_column[columns[i]]=[error_reason,row[-1]]




                    if(rule[0]=='DataType'):

                        if (rule[1].lower()=='int'):
                            local_correct_record=check_datatype(row[i],int)

                            if global_correct_record==True:
                                global_correct_record=local_correct_record
                            if not local_correct_record:
                                error_reason.append('DataType')
                                error_column[columns[i]]=[error_reason,row[-1]]

                        elif (rule[1].lower()=='string'):
                            local_correct_record=check_datatype(row[i],str)

                            if global_correct_record==True:
                                global_correct_record=local_correct_record
                            if not local_correct_record:
                                error_reason.append('DataType')
                                error_column[columns[i]]=[error_reason,row[-1]]

                        elif (rule[1].lower()=='float'):
                            local_correct_record=check_datatype(row[i],float)

                            if global_correct_record==True:
                                global_correct_record=local_correct_record
                            if not local_correct_record:
                                error_reason.append('DataType')
                                error_column[columns[i]]=[error_reason,row[-1]]


                    if(rule[0]=='RegExCheck' ):


                        if (row[i] is None):
                            local_correct_record=False
                        else:
                            local_correct_record = check_regex(row[i],rule[1])
                        if global_correct_record==True:
                            global_correct_record=local_correct_record

                        if not local_correct_record:
                            error_reason.append('RegExCheck')
                            error_column[columns[i]]=[error_reason,row[-1]]


                    if(rule[0]=='CustomSQL'):


                        validation_set=[val[0] for val in curs1.execute(rule[1]).fetchall()]

                        local_correct_record = check_custom_sql(row[i],validation_set)
                        if global_correct_record==True:
                            global_correct_record=local_correct_record

                        if not local_correct_record:
                            error_reason.append('CustomSQL')
                            error_column[columns[i]]=[error_reason,row[-1]]



            if not global_correct_record:
                for key,value in error_column.items():
                    
                    curs1.execute('insert into stg_dq_error values (\''+table[0]+'\',\''+key+'\',\''+str(error_column[key][1])+'\',\''+','.join(error_column[key][0])+'\',\''+time+'\')')
#                     print('inserting record to error table')

    print('Script executed succefully')
except Exception as e:
    print(e)