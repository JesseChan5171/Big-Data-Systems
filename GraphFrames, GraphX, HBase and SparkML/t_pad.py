import numpy as np
i = 1

with open('g2','a') as g2:
    with open('googlebooks-eng-all-1gram-20120701-b', 'r') as f:
        lines = f.readlines()
       
        for line in lines:
            line = line.strip().split('\t')
            
            line[2] = line[2].zfill(5)
            line.insert(0, int(i))
            np.savetxt(g2, [line], delimiter='\t', fmt = '%s', newline = '\n') 
            i = i + 1
            
            # if i > 20 :
                # break


scan 'g3_tb', {FILTER=>"SingleColumnValueFilter('cf','year',=,'binary:1671') AND SingleColumnValueFilter('cf','match_count',>,'binary:00100')",COLUMNS => ['cf']}

scan '<table_name>',{FILTER=>"SingleColumnValueFilter('<column_family>','<column_qualifier>',<comp_operator>,'binary:<qualifier_value>') AND SingleColumnValueFilter('<column_family>','<column_qualifier>',<comp_operator>,'binary:<qualifier_value>')",COLUMNS=>['column_family']}

    
    
get 'g3_tb','rowkey',{FILTER => "SingleColumnValueFilter('cf', 'year', = , 'binary:1671')"}

scan 'g3_tb', {FILTER => "SingleColumnValueFilter('cf', 'match_count', > , 'binary:00100')"}

scan 'g3_tb',{LIMIT => 10, FILTER =>"(PrefixFilter ())"}



scan 'g3_tb',{STARTROW => 'a1s1', ENDROW => 'a4s1', FILTER => "SingleColumnValueFilter('cf', 'year', = , 'binary:1671')"}


scan 'g3_tb', {FILTER=>"SkipFilter(SingleColumnValueFilter('cf','year',=,'binary:1671') AND SingleColumnValueFilter('cf','match_count',>,'binary:00100'))",COLUMNS => ['cf']}