hive -e "describe thomas_tostratos.stratos_1" | tee ./headers/header_1.txt
hive -e "describe thomas_tostratos.stratos_2a_perjob" | tee ./headers/header_2b_perjob.txt
hive -e "describe thomas_tostratos.stratos_2a_pertask" | tee ./headers/header_2a_pertask.txt
hive -e "describe thomas_tostratos.stratos_3" | tee ./headers/header_3.txt
