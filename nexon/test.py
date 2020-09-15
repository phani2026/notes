import pandas as pd
from numpy.random import randint
import time


df = pd.DataFrame(columns=['cust', 'qty1', 'qty2'])
vertical_stack = pd.DataFrame(columns=['cust', 'qty1', 'qty2'])

df2 = pd.DataFrame(columns=['cust', 'm1'])



for i in range(1000000000):
    df.loc[i] = ['name' + str(i), '1-'+str(i), '2-'+str(i) ]
    vertical_stack = pd.concat([vertical_stack, df], axis=0)
    df = pd.concat([vertical_stack, df], axis=0)
    print(len(vertical_stack),len(df),i)
    if(len(vertical_stack)>1000000000):
        break
    # if i%1000==0 :
    #     df2.loc[i] = ['name' + str(i), '1-'+str(i)+str(randint(10)) ]
    #     print(i)

#vertical_stack = pd.concat([survey_sub, survey_sub_last10], axis=0)

print('Done')