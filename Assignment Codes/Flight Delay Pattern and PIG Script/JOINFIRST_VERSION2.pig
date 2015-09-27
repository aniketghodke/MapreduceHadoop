/*Loasing the required files */

REGISTER  file:/home/hadoop/lib/pig/piggybank.jar
DEFINE  CSVLoader  org.apache.pig.piggybank.storage.CSVLoader;

/* Setting the number of reduce tasks */
set default_parallel 10;

/*Loading the flight data */
flight_data = LOAD 's3://assignment34/data.csv' USING CSVLoader();

/* Getting the required data for flight1 */
flight1_required = FOREACH flight_data GENERATE (int)$0 as year_f1,(int)$2 as month_f1,$5 as flight_date_f1,(chararray)$11 as origin_f1,(chararray)$17 as dest_f1,(int)$24 as dep_time_f1,(int)$35 as arr_time_f1,(float)$37 as arr_delay_time_f1, (float)$41 as cancelled_f1, (float)$43 as diverted_f1;

/* Getting the required data for flight2 */
flight2_required = FOREACH flight_data GENERATE (int)$0 as year_f2,(int)$2 as month_f2,$5 as flight_date_f2,(chararray)$11 as origin_f2,(chararray)$17 as dest_f2,(int)$24 as dep_time_f2,(int)$35 as arr_time_f2,(float)$37 as arr_delay_time_f2, (float)$41 as cancelled_f2, (float)$43 as diverted_f2;

/* Getting the requied flights only f1*/
flight1_filtered = FILTER flight1_required BY cancelled_f1 == 0.00 AND diverted_f1 == 0.00 AND origin_f1 == 'ORD' AND dest_f1 != 'JFK' ;

/* Getting the required flights only f2*/
flight2_filtered = FILTER flight2_required BY cancelled_f2 == 0.00 AND diverted_f2 == 0.00 AND dest_f2 == 'JFK' AND origin_f2 != 'ORD';  

/* Joining the data  */
join_data = JOIN flight1_filtered BY (dest_f1, flight_date_f1) , flight2_filtered BY (origin_f2, flight_date_f2);

/* filtering the flights with arr time < dep time */
proper_time = FILTER join_data BY arr_time_f1 < dep_time_f2;

/* filrtering the required time */ 
date = FILTER proper_time BY ((year_f1 == 2007 AND month_f1 >= 6) OR (year_f1 == 2008 AND month_f1 <= 5));

/* Addint the time */ 
add_times = FOREACH date GENERATE (float)(arr_delay_time_f1 + arr_delay_time_f2) as total_delay;

/* Grouping the data */ 
group1 = GROUP add_times ALL;

/* Calculatig the final average */ 
average = FOREACH group1 GENERATE AVG(add_times.total_delay);

/* Storing the final average */
STORE average INTO 's3://pigassignment3/JoinVersion1';
