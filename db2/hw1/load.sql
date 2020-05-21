load from "./arcos-ny-statewide-itemized.csv" 
of del modified by DATEFORMAT="MMDDYYYY"  MESSAGES load.msg insert INTO CSE532.DEA_NY;