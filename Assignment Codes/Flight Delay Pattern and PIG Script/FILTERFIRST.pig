/*Loading the required files*/

REGISTER  file:/home/hadoop/lib/pig/piggybank.jar
DEFINE  CSVLoader  org.apache.pig.piggybank.storage.CSVLoader;

/*Setting the number iof reduce task*/
set default_parallel 10;

/*Loading the flight data */
flight_data = LOAD 's3://assignment34/data.csv' USING CSVLoader();

/* Getting the required data for flight1 */
flight1_required = FOREACH flight_data GENERATE (int)$0 as year_f1,(int)$2 as month_f1,$5 as flight_date_f1,(chararray)$11 as origin_f1,(chararray)$17 as dest_f1,(int)$24 as dep_time_f1,(int)$35 as arr_time_f1,(float)$37 as arr_delay_time_f1, (float)$41 as cancelled_f1, (float)$43 as diverted_f1;

/* Getting the required data for flight2 */
flight2_required = FOREACH flight_data GENERATE (int)$0 as year_f2,(int)$2 as month_f2,$5 as flight_date_f2,(chararray)$11 as origin_f2,(chararray)$17 as dest_f2,(int)$24 as dep_time_f2,(int)$35 as arr_time_f2,(float)$37 as arr_delay_time_f2, (float)$41 as cancelled_f2, (float)$43 as diverted_f2;

/* Filtering out data for flight 1 on cacelled and diverted and origin and destination and year*/
flight1_filtered = FILTER flight1_required BY cancelled_f1 == 0.00 AND diverted_f1 == 0.00 AND origin_f1 == 'ORD' AND dest_f1 != 'JFK' AND ((year_f1 == 2007 AND month_f1 >= 6) OR (year_f1 == 2008 AND month_f1 <= 5));

/* Filtering out data for flight 2 on cacelled and diverted and origin and destination and year*/
flight2_filtered = FILTER flight2_required BY cancelled_f2 == 0.00 AND diverted_f2 == 0.00 AND dest_f2 == 'JFK' AND origin_f2 != 'ORD' AND ((year_f2 == 2007 AND month_f2 >= 6) OR (year_f2 == 2008 AND month_f2 <= 5));  

/* Joining the data */
join_data = JOIN flight1_filtered BY (dest_f1, flight_date_f1) , flight2_filtered BY (origin_f2, flight_date_f2);

/* Filtering out time arr < time dep */
proper_time = FILTER join_data BY arr_time_f1 < dep_time_f2;

/* Adding total delay */
added_times = FOREACH proper_time GENERATE (float)(arr_delay_time_f1 + arr_delay_time_f2) as total_delay;

/* Grouping to apply average function */
group1 = GROUP added_times ALL;

/* calculating final average */
final_average = FOREACH group1 GENERATE AVG(added_times.total_delay);

/* Storing the output to the file */
STORE final_average INTO 's3://pigassignment3/FilterFirst';

