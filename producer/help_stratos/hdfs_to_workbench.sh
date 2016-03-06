
#rm -r ./output/request_series
#hadoop dfs -copyToLocal '/user/tnystrand/stratos_1/1_request_series' output/request_series
#rm -r ./output/2a_pertask
#hadoop dfs -copyToLocal '/user/tnystrand/stratos_1/2a_pertask' output/2a_pertask
rm -r ./output/2b_perjob
hadoop dfs -copyToLocal '/user/tnystrand/stratos_1/2b_perjob' output/2b_perjob
#rm -r ./output/3_host_series
#hadoop dfs -copyToLocal '/user/tnystrand/stratos_1/3_host_series' output/3_host_series
